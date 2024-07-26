from setuptools import setup

setup(
    name='tecton_parallel_retrieval',
    version='0.0.17-beta.1',
    description='[private preview] Parallel feature retrieval for Tecton',
    author='Tecton',
    packages=['tecton_parallel_retrieval', 'tecton_parallel_retrieval.spark', 'tecton_parallel_retrieval.rift'],
    license="Apache License 2.0",
    install_requires=[
        "tecton>=0.9,<0.10",
    ],
    extras_require={
        'spark': ["databricks-sdk", "pyspark"],
    },
    setup_requires=["setuptools", "wheel"],
    url="https://tecton.ai",
    python_requires=">=3.7",
)
