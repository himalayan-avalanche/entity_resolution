from setuptools import setup

setup(
    name='absmlir',
    version='0.1.0',
    description='Identity Resolution Library',
    url='https://github.com/ashwinimaurya/entity_resolution/tree/main',
    author='Ashwini Maurya',
    author_email='akmaurya07@gmail.com',
    license="""Copyright © 2023 Ashwini Maurya. All rights reserved. .""",
    packages=['absmlir'],
    install_requires=['pandas',
                      'numpy',
                      'fastDamerauLevenshtein',
                      'Levenshtein',
                      'rapidfuzz',
                      'pyblake2',
                      'phonenumbers',
                      'jellyfish',
                      'phonetics',
                      'pyblake2',
                      'wheel'
                        ],
)
