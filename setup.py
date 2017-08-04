from setuptools import setup, find_packages

setup(
    name='brightside',
    description='Provides a command dispatcher and task queue to support CQRS and microservices ',
    long_description="""See the Github project page (https://github.com/BrigherCommand/Brightside) for more on Brightside""",
    license='MIT',
    keywords=['brightside', 'messaging', 'command', 'dispatcher', 'invoker',  'CQRS', 'microservices'],
    version='0.1',
    author='Ian Cooper',
    author_email='ian_hammond_cooper@yahoo.co.uk',
    url='https://github.com/BrigherCommand/Brightside',
    packages=find_packages(exclude=["tests", "examples"]),
    install_requires=['ez_setup', 'kombu', 'poll', 'mock'],
    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt', '*.rst'],
    },
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Distributed Computing",
    ]
)
