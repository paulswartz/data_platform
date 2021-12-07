
from setuptools import setup

# metadata in setup.cfg
setup(
  name='MBTA Data Platform',
  install_requires=[
    'alembic==1.7.5',
    'asn1crypto==1.4.0',
    'boto3==1.20.16',
    'botocore==1.23.16',
    'greenlet==1.1.2',
    'importlib-metadata==4.8.2',
    'importlib-resources==5.4.0',
    'jmespath==0.10.0',
    'Mako==1.1.6',
    'MarkupSafe==2.0.1',
    'psycopg2-binary==2.9.2',
    'python-dateutil==2.8.2',
    'python-dotenv==0.19.2',
    's3transfer==0.5.0',
    'scramp==1.4.1',
    'six==1.16.0',
    'SQLAlchemy==1.4.27',
    'typing-extensions==4.0.0',
    'urllib3==1.26.7',
    'zipp==3.6.0',
  ],
  extras_require={
    'local': [],
  },
)
