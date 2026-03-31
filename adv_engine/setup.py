"""
Setup configuration for ADV Buying Signal Engine
"""

from setuptools import setup, find_packages

setup(
    name="adv-engine",
    version="1.0.0",
    description="ADV Buying Signal Engine - Identifies high-potential financial advisor firms",
    author="I Squared Capital",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "Flask>=3.0.0",
        "Flask-CORS>=4.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
        ]
    },
)
