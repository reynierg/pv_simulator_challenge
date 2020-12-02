import datetime
import getopt
import logging
import pathlib
import queue
import time
import typing

import tenacity

from services.meter.delayed_keyboard_interrupt import DelayedKeyboardInterrupt
from services.meter.typing_custom_protocols import MessagingThreadProtocol


class MainLoop:
    """Handles the user's provided arguments.

    If the user provided arguments are valid, it will either:
    -Print help message.
    -Run pre-configured tests cases.
    -Run the Meter simulator.
    """

    def __init__(self,
                 logger_name: str,
                 max_meter_queue_size: int,
                 current_dir: pathlib.Path,
                 must_exit_after_24h: bool,
                 messaging_thread_provider: typing.Callable[[queue.Queue], MessagingThreadProtocol],
                 meter_values_generator: typing.Callable[[], typing.Generator[int, None, None]],
                 default_action_handler: typing.Callable[[], None] = None,
                 tests_modules_names_provider: typing.Callable[[], typing.List[str]] = lambda: []):
        """

        Parameters
        ----------
        logger_name : str
            Name of the logger to be used
        max_meter_queue_size : int
            Size of the `queue.Queue` that will hold the Meter's generated values.
        current_dir : pathlib.Path
            Refers the parent directory
        must_exit_after_24h : bool
            If True, the app will abort its execution after 24h of being running
        messaging_thread_provider : callable
            Method to be executed to acquire an instance of a `MessagingThread`.
        meter_values_generator : generator
            Generates a value between [0, 9000] everytime it's iterated over.
        default_action_handler: callable, optional
            When provided, specifies the default action handler to be used when a specified 
            action is not recognized.
        tests_modules_names_provider : callable, optional
            Provides the list of tests modules to run.
        """

        # Maps Command Line Interface allowed actions to actions handlers.
        self._action_handlers: typing.Dict[str, typing.Callable] = {
            "help": self._usage_handler,
            "test": self._run_tests_handler,
            "start": self._run_meter_simulator_handler
        }

        self._log: logging.Logger = logging.getLogger(logger_name)
        self._log.debug(f"{self.__class__.__name__}.__init__()")

        self._max_meter_queue_size = max_meter_queue_size
        self._meter_values_queue: typing.Optional[queue.Queue] = None
        # Establish how to re-try to add a meter value to the queue when it's full.
        # Also specifies when it should stop from re-trying. Re-tries would be aborted, if was specified to abort execution 
        # after 24h and they have already lapsed:
        self._retry_policy = tenacity.Retrying(
            wait=tenacity.wait_fixed(0.5),  # Wait 0.5sec before try again to add the generated meter value to the queue
            retry=tenacity.retry_if_exception_type(queue.Full),
            stop=tenacity.stop_any(self._must_exit),
            after=lambda _, __, ___: self._log.warning("Meter's values queue is Full!!!")
        )

        self._current_dir = current_dir

        self._must_exit_after_24h: bool = must_exit_after_24h
        self._start_datetime: datetime = datetime.datetime.now()
        self._stop_datetime: datetime = self._start_datetime + datetime.timedelta(hours=24)
        # self._stop_datetime: datetime = self._start_datetime + datetime.timedelta(minutes=2)

        self._messaging_thread_provider: typing.Callable[[queue.Queue], MessagingThreadProtocol] = \
            messaging_thread_provider

        self._meter_values_generator: typing.Callable[[], typing.Generator[int, None, None]] = \
            meter_values_generator
        self._default_action_handler: typing.Callable[[], None] = \
            default_action_handler or self._usage_handler

        self._tests_modules_names_provider: typing.Callable[[], typing.List[str]] = \
            tests_modules_names_provider

        self._messaging_thread: typing.Optional[MessagingThreadProtocol] = None

    def _must_exit(self, retry_state: typing.Optional[tenacity.RetryCallState] = None) -> bool:
        """Determines if the process's main execution thread must abort its execution

        Parameters
        ----------
        retry_state : tenacity.RetryCallState
            Contains information related to the re-trying status.

        Returns
        -------
        bool
            Will be True if the process was configured to abort execution after 24h,
            and that time has already elapsed.
        """

        self._log.debug(f"{self.__class__.__name__}._must_exit()")
        should_exit, now = self._check_if_must_exit()
        if should_exit:
            self._log.info(f"Aborting execution after 24h running. "
                           f"Current time: {now.strftime('%d.%m.%Y %H:%M:%S')}")

        return should_exit

    def _check_if_must_exit(self) -> typing.Tuple[bool, datetime.datetime]:
        """Determines if the process's main execution thread must abort its execution

        Returns
        -------
        tuple:
            The first element will be a boolean value indicating if the process must abort execution.
            The second element would contain the now datetime.
        """

        self._log.debug(f"{self.__class__.__name__}._check_if_must_exit()")
        now_datetime = datetime.datetime.now()
        return self._must_exit_after_24h and now_datetime > self._stop_datetime, now_datetime

    def _initialize_messaging_thread(self) -> None:
        """Initialize and fires the execution of a MessagingThread.

        The created execution thread will be responsible of send generated Meter's values to the
        PV Simulator service.
        """

        self._log.debug(f"{self.__class__.__name__}._initialize_messaging_thread()")
        self._messaging_thread = self._messaging_thread_provider(self._meter_values_queue)
        self._messaging_thread.start()

    def _usage_handler(self) -> None:
        """Prints to the STDOUT(terminal) information related to the program usage.
        """

        self._log.debug(f"{self.__class__.__name__}._usage_handler()")
        print("Usage:")
        print("python meter_simulator.py -a <start/test>")
        print("")
        print("Examples:")
        print("\tFor run tests:")
        print("\t\tpython -a test")
        print("\tFor run meter simulator:")
        print("\t\tpython -a start")

    def _run_tests_handler(self) -> None:
        """Executes the pre-configured tests using the nose test runner.

        It will run the test cases discovered in the modules pre-configured and using code coverage.
        Will generate several HTML pages, with statistics related to the the tests execution coverage.
        """

        self._log.debug(f"{self.__class__.__name__}._run_tests_handler()")
        print("run_tests() was CALLED")
        # utils.initialize_nose_logger()
        # utils.initialize_logger()

        import nose
        import coverage
        cov = coverage.Coverage(source=["services.meter"], omit=["*/tests/*"])
        # Exclude lines of code containing meta variables and dandy methods like __str__, __author__
        # cov.exclude("__(.')__")
        cov.set_option('report:show_missing', True)
        cov.erase()
        cov.start()

        test_modules_names = self._tests_modules_names_provider()
        print("Tests modules to run:", test_modules_names)
        args = [""]
        args.extend(test_modules_names)
        nose.run(argv=args)

        cov.stop()
        cov.html_report(directory=str(self._current_dir / "data" / "coverage_html"))
        cov.save()
        cov.report()

    def _add_meter_value_to_queue(self, meter_value: int) -> None:
        """Adds a Meter's generated value to the Meter's queue.

        Parameters
        ----------
        meter_value : int
            Values to be added to the Meter's queue.
        """

        self._log.debug(f"{self.__class__.__name__}._add_meter_value_to_queue(meter_value={meter_value})")
        # If the queue's size is already `self._max_meter_queue_size`, it will block for a second, for an empty
        # slot. If after 1 sec an empty slot is not available yet, it will throw queue.Full exception.
        self._meter_values_queue.put(meter_value, block=True, timeout=1)

    def _run_meter_simulator_handler(self) -> None:
        """Handles the "start" action, when specified by the user while executing the CLI.

        -Initializes the `MessagingThread` thread execution, that will handle the Meter's generated values.
        -Periodically generates Meter's values.
        -Aborts execution if required.
        """

        self._log.debug(f"{self.__class__.__name__}._run_meter_simulator_handler()")
        self._log.info(f"Starting execution at: {self._start_datetime.strftime('%d.%m.%Y %H:%M:%S')}")

        generator: typing.Generator[int, None, None] = self._meter_values_generator()

        # Initialize a thread-safe queue to push generated Meter's power values to the messaging thread.
        # The queue will have a fixed size to prevent Memory Overflow from happen, in the case that the
        # Messaging Thread gets stuck processing the generated Meter power values(For example if it fails
        # to communicate with the AMQP server, what will cause to re-try), while the main thread continues
        # to generate new values at a fast rate:
        self._meter_values_queue = queue.Queue(self._max_meter_queue_size)
        try:
            try:
                # Shield `MQSender` and `MessagingThread` initialization from termination:
                with DelayedKeyboardInterrupt():
                    # Initialize the messaging thread that will post to a AMQP broker the generated
                    # Meter's power values:
                    self._initialize_messaging_thread()
            except KeyboardInterrupt:
                print(f'!!! got KeyboardInterrupt during `MessagingThread` initialization')
                raise

            # Execute the main loop. It will be executed uninterruptedly unless either, the process is killed or
            # was specified to abort execution after 24 hours:
            for meter_value in generator:
                self._log.debug(f"Generated meter_value: {meter_value} Watts")
                # Send the generated Meter's power value to the messaging thread to be processed.
                # If the queue is Full, it will sleep for 0.5sec, and continue retrying until either,
                # a slot is free, or the process is killed:
                self._retry_policy.call(self._add_meter_value_to_queue, meter_value)
                # Wait 2 seconds for generate a new Meter's power values:
                time.sleep(2)

                if self._must_exit():
                    break

        except tenacity.RetryError:
            # If we're here, is because the allocated execution time has lapsed.
            self._log.info("Giving up trying to put the Meter's generated power value in the queue, "
                           "because the allocated execution time has elapsed.")

        except KeyboardInterrupt:
            self._log.warning("Required to abort...")

        finally:
            try:
                # Shield the resources de-allocation from termination:
                with DelayedKeyboardInterrupt():
                    if self._messaging_thread is not None:
                        # Ask the messaging thread to abort execution:
                        self._messaging_thread.stop()
                        self._messaging_thread.join()

                        if self._messaging_thread.mq_sender is not None:
                            try:
                                self._messaging_thread.mq_sender.close_connection()
                            except Exception:
                                self._log.exception("Error trying to close the connection to the AMQP broker:")
            except KeyboardInterrupt:
                print(f'!!! got KeyboardInterrupt during stop')

    def _parse_arguments(self, sys_argv: typing.List[str]) -> str:
        """Parse the user supplied arguments, when executed the CLI.

        Parameters
        ----------
        sys_argv : list
            Contains the user supplied arguments.

        Returns
        -------
        str
            The action specified by the user. If a valid action was not specified, an empty str is returned.
        """

        self._log.debug(f"{self.__class__.__name__}._parse_arguments(sys_argv={sys_argv})")
        action = ""
        try:
            options, args = getopt.getopt(sys_argv[1:], "a:h", ["action=", "help"])
            for opt, arg in options:
                if opt in ("h", "--help"):
                    action = "help"
                    break

                if opt in ("-a", "--action"):
                    action = arg
                    break

        except getopt.GetoptError:
            self._log.exception("Error:")
            # Error will be handled by caller
            pass

        return action

    def handle_arguments(self, sys_argv: typing.List[str]) -> None:
        """Handle the arguments provided by the user while executing the CLI.

        Parameters
        ----------
        sys_argv : list
            User provided arguments.
        """

        self._log.debug(f"{self.__class__.__name__}.handle_arguments(sys_argv={sys_argv})")
        action = self._parse_arguments(sys_argv)
        action_handler = self._action_handlers.get(action)
        if action_handler is None:
            print(f"Invalid command found in {sys_argv}")
            self._default_action_handler()
        else:
            action_handler()
