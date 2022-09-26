#!/usr/bin/env python

from setuptools import setup

setup(name='logstar-receiver',
        version='1.0',
        description='supporting library for bonares/zalf repository',
        url='https://github.com/zalf-rdm/Logstar-online-Stream/',
        author='marcel wallschlaeger',
        author_email='marcel.wallschlaeger@zalf.de',
        zip_safe = False,
        packages=['src','processing_steps'],
      )
