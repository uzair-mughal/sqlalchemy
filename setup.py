from distutils.core import setup
from setuptools import find_packages, setup


setup(
    name="sqlalchemy_facade",
    packages=find_packages(),
    version="0.0.1",
    license="MIT",
    description="",
    author="Uzair Ahmed Mughal",
    author_email="uzam.dev@gmail.com",
    url="",
    download_url="",
    keywords=["AsyncIO", "SQLAlchemy"],
    install_requires=["sqlalchemy==2.0.22", "asyncpg"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
    ],
)
