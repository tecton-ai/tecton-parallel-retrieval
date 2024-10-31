from setuptools import setup, find_packages

setup(
    name='tecton_parallel_retrieval',
    version='0.1.0',
    description='[private preview] Parallel feature retrieval for Tecton',
    author='Tecton',
    packages=find_packages(),
    license="Apache License 2.0",
    install_requires=[
        "tecton>=1.0",
    ],
    extras_require={
        'spark': ["databricks-sdk", "pyspark"],
    },
    setup_requires=["setuptools", "wheel"],
    url="https://tecton.ai",
    python_requires=">=3.7",
)
