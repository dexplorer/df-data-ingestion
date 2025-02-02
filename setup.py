import setuptools

setuptools.setup(
    name="ingest_app",
    version="1.0.0",
    scripts=["./scripts/ingest_app"],
    author="Rajakumaran Arivumani",
    description="Data ingestion app install.",
    url="https://github.com/dexplorer/df-data-ingestion",
    packages=[
        "ingest_app",
        "ingest_app.ingest_spark",
    ],
    # packages = find_packages(),
    install_requires=[
        "pyspark==3.5.4",
        "setuptools",
        "requests",
        "utils@git+https://github.com/dexplorer/utils#egg=utils-1.0.2",
        "metadata@git+https://github.com/dexplorer/df-metadata#egg=metadata-1.0.8",
        "app_calendar@git+https://github.com/dexplorer/df-app-calendar#egg=app_calendar-1.0.2",
    ],
    python_requires=">=3.12",
)
