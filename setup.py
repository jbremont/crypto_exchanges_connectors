from setuptools import setup, find_packages
from exchanges import __version__

setup(
    name='exchanges',
    version=__version__,
    description='exchange adapters',
    author='Aye-Jay',
    include_package_data=True,
    packages=find_packages(),
    install_requires=[
        'Flask==0.12.2',
        'pandas==0.20.1',
        'requests==2.18.4',
        'aj_sns==0.0.56',
        'networkx==2.1',
        'ethereum==2.3.1',
        'rlp==0.6.0',
        'python-binance',
        'ccxt==1.12.10',
        'selenium==3.12.0',
        'pusher==2.0.1',
        'python-quoine==0.1.4',
        'bittrex-websocket==1.0.6.2',
        'python-bittrex==0.3.0',
        'websocket-client==0.48.0',
        'pycrypto',
        'matplotlib'])
