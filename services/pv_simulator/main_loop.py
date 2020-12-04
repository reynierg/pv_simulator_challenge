import datetime
import getopt
import logging
import pathlib
import time
import typing

from pydantic import ValidationError
import tenacity

from services.pv_simulator.delayed_keyboard_interrupt import DelayedKeyboardInterrupt
from services.pv_simulator.models import MsgPayloadModel
from services.pv_simulator.typing_custom_protocols import (
    MQReceiverProtocol,
    PVPowerValueCalculatorProtocol
)


class MainLoop:
    """Handles the user's provided arguments.

    If the user provided arguments are valid, it will either:
    -Print help message.
    -Run pre-configured tests cases.
    -Run the PV simulator.
    """

    def __init__(self,
                 logger_name: str,
                 results_logger_name: str,
                 current_dir: pathlib.Path,
                 must_exit_after_24h: bool,
                 mq_receiver_provider: typing.Callable[[typing.Callable[[], bool]], MQReceiverProtocol],
                 pv_power_calculator_provider: typing.Callable[[], PVPowerValueCalculatorProtocol],
                 default_action_handler: typing.Callable[[], None] = None,
                 tests_modules_names_provider: typing.Callable[[], typing.List[str]] = lambda: []):
        """

        Parameters
        ----------
        logger_name : str
            Name of the logger to be used.
        results_logger_name : str
            Name of the logger to be used that handles the results.
        current_dir : pathlib.Path
            Refers the parent directory
        must_exit_after_24h : bool
            If True, the app will abort its execution after 24h of being running
        mq_receiver_provider : callable
            Method to be executed to acquire a concrete instance of a `MQReceiver`.
        pv_power_calculator_provider : callable
            Method to be executed to acquire an instance of a `PVPowerValueCalculator`.
        default_action_handler: callable, optional
            When provided, specifies the default action handler to be used when a specified
            action is not recognized.
        tests_modules_names_provider : callable, optional
            Provides the list of tests modules to run.
        """

        # Maps Command Line Interface allowed actions to actions handlers.
        self._action_handlers: typing.Dict[str, typing.Callable[[], None]] = {
            "help": self._usage_handler,
            "test": self._run_tests_handler,
            "start": self._run_pv_power_simulator_handler
        }

        self._log: logging.Logger = logging.getLogger(logger_name)
        self._log.debug(f"{self.__class__.__name__}.__init__()")

        self._results_log: logging.Logger = logging.getLogger(results_logger_name)

        self._current_dir: pathlib.Path = current_dir

        self._must_exit_after_24h: bool = must_exit_after_24h
        self._start_datetime: datetime = datetime.datetime.now()
        self._stop_datetime: datetime = self._start_datetime + datetime.timedelta(hours=24)
        # self._stop_datetime: datetime = self._start_datetime + datetime.timedelta(minutes=2)

        self._mq_receiver_provider: typing.Callable[[typing.Callable[[], bool]], MQReceiverProtocol] = \
            mq_receiver_provider
        self._mq_receiver: typing.Optional[MQReceiverProtocol] = None

        self._pv_power_calculator_provider: typing.Callable[[], PVPowerValueCalculatorProtocol] = \
            pv_power_calculator_provider
        self._pv_power_value_calculator: typing.Optional[PVPowerValueCalculatorProtocol] = None

        self._default_action_handler: typing.Callable[[], None] = \
            default_action_handler or self._usage_handler

        self._tests_modules_names_provider: typing.Callable[[], typing.List[str]] = \
            tests_modules_names_provider

        self._meter_value_observers: typing.List = list()

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

    def _must_exit(self) -> bool:
        """Determines if the process's main execution thread must abort its execution

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

    def _usage_handler(self) -> None:
        """Prints to the STDOUT(terminal) information related to the program usage.
        """

        self._log.debug(f"{self.__class__.__name__}._usage_handler()")
        print("Usage:")
        print("python pv_simulator.py -a <start/test>")
        print("")
        print("Examples:")
        print("\tFor run tests:")
        print("\t\tpython -a test")
        print("\tFor run PV power simulator:")
        print("\t\tpython -a start")

    def _run_tests_handler(self) -> None:
        """Executes the pre-configured tests.

        It will run the test cases discovered in the modules pre-configured and using code coverage.
        Will generate several HTML pages, with statistics related to the the tests execution coverage.
        """

        print("run_tests() was CALLED")
        # utils.initialize_nose_logger()
        # utils.initialize_logger()

        import nose
        import coverage
        from services.pv_simulator.tests.unit import constants_for_tests
        cov = coverage.Coverage(source=["services.pv_simulator"], omit=["*/tests/*"])
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

    def _run_pv_power_simulator_handler(self):
        """Handles the "start" action, when specified by the user while executing the CLI.

        -Periodically gets Meter's generated values from a AMQP broker.
        -Periodically generates PV's power values.
        -Aborts execution if required.
        """

        self._log.debug(f"{self.__class__.__name__}._run_pv_power_simulator_handler()")
        self._log.info(f"Starting execution at: {self._start_datetime.strftime('%d.%m.%Y %H:%M:%S')}")
        try:
            try:
                # Shield `MQReceiver` initialization from termination:
                with DelayedKeyboardInterrupt():
                    # Initialize the `MQReceiver` that will be used to read messages from the AMQP broker:
                    self._mq_receiver = self._mq_receiver_provider(self._must_exit)
            except KeyboardInterrupt:
                print(f'!!! got KeyboardInterrupt during `MQReceiver` initialization')
                raise

            self._pv_power_value_calculator: PVPowerValueCalculatorProtocol = \
                self._pv_power_calculator_provider()

            while True:
                msg_payload = self._mq_receiver.get_message(ack_on_receive=False)
                self._log.info(f"msg_payload={msg_payload}")
                if msg_payload is not None:
                    # Generate the PV power value. The generated PV power value would depends in the current
                    # time of the day, trying to replicate the PV power values depicted in the
                    # PV Power(kW)/DayTime diagram in the received PV Simulator Challenge document.
                    # The generated PV power value will reach its peak value around 14:24:
                    try:
                        msg_payload_model: MsgPayloadModel = MsgPayloadModel(**msg_payload)
                    except ValidationError:
                        self._log.exception("Message's payload is invalid:")
                    else:
                        now_datetime = datetime.datetime.now()
                        self._log.debug(f"now_datetime: {now_datetime}")
                        now_in_minutes_from_0000 = now_datetime.hour * 60 + now_datetime.minute
                        pv_power_value = self._pv_power_value_calculator.get_pv_power_value(now_in_minutes_from_0000)
                        meter_power_in_kw = int(msg_payload_model.meter_power) / 1000
                        pv_power_value_in_kw = pv_power_value / 1000
                        # Write to a file: current timestamp, meter power value acquired from RabbitMQ,
                        # generated PV power value and the sum of the powers (meter + PV):
                        self._results_log.info(f"{now_datetime.strftime('%d.%m.%Y %H:%M:%S')},"
                                               f"{meter_power_in_kw:.2f},"
                                               f"{pv_power_value_in_kw:.2f},"
                                               f"{(meter_power_in_kw + pv_power_value_in_kw):.2f}")

                    delivery_tag = msg_payload.get(self._mq_receiver.DELIVERY_TAG, None)
                    if delivery_tag is not None:
                        self._mq_receiver.ack_message(delivery_tag)

                    if self._must_exit():
                        break

                time.sleep(2)

        except tenacity.RetryError:
            # If we're here, is because the allocated execution time has lapsed.
            self._log.info("Giving up trying to get a Meter's generated power value from the AMQP broker, "
                           "because the allocated execution time has elapsed.")

        except KeyboardInterrupt:
            self._log.warning("Required to abort...")

        finally:
            try:
                # Shield the resources de-allocation from termination:
                with DelayedKeyboardInterrupt():
                    if self._mq_receiver is not None:
                        try:
                            self._mq_receiver.close_connection()
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
            action = ""
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
