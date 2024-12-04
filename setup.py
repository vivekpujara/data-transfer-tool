from setuptools import setup, find_packages

setup(
    name="data-transfer-tool",
    version="0.1.0",
    description="Tool to transfer data between HPC clusters using AWS S3",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Vivek Pujara",
    author_email="vivekpujara.vp@gmail.com",
    url="https://github.com/vivekpujara/data-transfer-tool",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "tqdm",
        "botocore",
    ],
    entry_points={
        "console_scripts": [
            "data-transfer=data_transfer_tool.cli:main",
        ],
    },
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
    ],
)
