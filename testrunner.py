import unittest


def discover_and_run():
    tests = unittest.TestLoader().discover('tests', pattern='*_tests.py')
    result = unittest.TextTestRunner(verbosity=2).run(tests)
    if result.wasSuccessful():
        return 0
    return 1


if __name__ == "__main__":
    discover_and_run()