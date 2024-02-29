from setuptools import setup

setup(name='tecton_parallel_retrieval',
            version='0.0.15',
            description='[private preview] Parallel feature retrieval on Databricks for Tecton',
            author='Tecton',
            packages=['tecton_parallel_retrieval'],
            license="Apache License 2.0",
            install_requires=[
                "databricks-sdk",
                "pyspark",
                "tecton ==0.8.*",
            ],
            setup_requires=["setuptools", "wheel"],
            url="https://tecton.ai",
            python_requires=">=3.7",
           )
