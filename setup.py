from setuptools import setup, find_packages

def get_long_description():
    with open('README.md') as f:
        return f.read()

setup(
    name='bqcsv',
    version='0.0.1',

    description="DEPT BigQuery CSV data exporter",
    long_description=get_long_description(),
    long_description_content_type='text/markdown',

    python_requires='~=3.6',

    install_requires=[
        'google-cloud-bigquery==1.23.1'
    ],

    dependency_links=[
    ],

    extras_require={
        'test': []
    },

    packages=find_packages(),

    author='bqcsv contributors',
    license='MIT',

    entry_points={
        'console_scripts': [
            'bqcsv = bqcsv.main:main',
        ],
    }
)
