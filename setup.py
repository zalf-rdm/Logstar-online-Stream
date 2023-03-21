#!/usr/bin/env python

from setuptools import setup

setup(
    name="logstar-receiver",
    version="1.1",
    description="supporting library for bonares/zalf repository",
    url="https://github.com/zalf-rdm/Logstar-online-Stream/",
    author="marcel wallschlaeger",
    author_email="marcel.wallschlaeger@zalf.de",
    zip_safe=False,
    packages=["logstar_stream", "logstar_stream.processing_steps"],
    install_requires=[
      "requests==2.28.1",
      "sqlalchemy==1.4.39",
      "pandas==1.4.3"
    ],
)
