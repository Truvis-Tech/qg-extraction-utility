from setuptools import setup, find_packages
import os

requirements = [
'argparse',
'beautifulsoup4==4.13.4',
'cachetools==5.5.2',
'certifi==2025.4.26',
'charset-normalizer==3.4.1',
'configparser',
'db-dtypes==1.4.2',
'google==3.0.0',
'google-api-core==2.24.2',
'google-auth==2.39.0',
'google-cloud==0.34.0',
'google-cloud-bigquery==3.31.0',
'google-cloud-core==2.4.3',
'google-crc32c==1.7.1',
'google-resumable-media==2.7.2',
'googleapis-common-protos==1.70.0',
'grpcio==1.72.0rc1',
'grpcio-status==1.72.0rc1',
'idna==3.10',
'numpy==2.2.5',
'packaging==25.0',
'pandas==2.2.3',
'proto-plus==1.26.1',
'protobuf==6.30.2',
'pyarrow==20.0.0',
'pyasn1==0.6.1',
'pyasn1_modules==0.4.2',
'python-dateutil==2.9.0.post0',
'pytz==2025.2',
'PyYAML==6.0.2',
'requests==2.32.3',
'rsa==4.9.1',
'six==1.17.0',
'soupsieve==2.7',
'typing_extensions==4.13.2',
'tzdata==2025.2',
'urllib3==2.4.0'
]

setup(
    name="trulens-extraction_utility",
    version="1.0.1",
    author="TruVis",
    packages=find_packages(include=["extraction_utility", "extraction_utility*"]),
    install_requires=requirements,
    python_requires=">=3.10",
    include_package_data=True,
    package_data={
        "extraction_utility": ["**/*"],
    },
    entry_points={
        "console_scripts": [
            "extraction = extraction_utility.info_schema:main",
            "logs = extraction_utility.logs:main"
        ]
    },
)