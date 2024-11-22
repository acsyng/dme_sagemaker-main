from setuptools import setup, find_packages

setup(
    name='sagemaker_dme_lib',
    version='1.0.0',
    packages=find_packages(),
    package_data={
        'libs': ['*']
    },
    egg_name='test'
)
