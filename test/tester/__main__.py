from termcolor import colored, cprint
import sys

from tester.context import reset, wait_for_all
from tester import tests


def main():
    cprint("Waiting for all services to be ready", "white", attrs=["bold"])
    wait_for_all()

    cprint("Running functional tests", "white", attrs=["bold"])

    running = colored("RUN", "yellow", attrs=["bold"])
    ok = colored("OK", "green", attrs=["bold"])
    fail = colored("FAIL", "red", attrs=["bold"])
    test_statuses = []
    n_failed_tests = 0

    test_sequence = [t for t in dir(tests) if t.startswith('test_')]
    for test_name in test_sequence:
        try:
            print("%-4s %s" % (running, test_name[5:]))
            sys.stdout.flush()
            getattr(tests, test_name)()
            test_statuses.append((test_name, 'ok'))
        except AssertionError as e:
            n_failed_tests += 1
            test_statuses.append((test_name, str(e)))
        reset()

    cprint("Test report", "white", attrs=["bold"])
    for test_name, test_status in test_statuses:
        if test_status == 'ok':
            print("%-4s %s" % (ok, test_name[5:]))
        else:
            print("%-4s %s\n%s" % (fail, test_name[5:], test_status))

    if n_failed_tests <= 0:
        cprint("All good!", "green", attrs=["bold"])
        exit(0)
    else:
        cprint("%d test(s) failed" % n_failed_tests, "red", attrs=["bold"])
        exit(1)


if __name__ == "__main__":
    main()
