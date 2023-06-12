from setuptools import find_packages, setup

setup(
    name="elt_xml_dag",
    packages=find_packages(exclude=["elt_xml_dag_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
