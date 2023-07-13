from setuptools import find_packages, setup

setup(
    name="dagster_cloud_demo",
    packages=find_packages(exclude=["dagster_cloud_demo_tests"]),
    install_requires=["dagster", "dagster-cloud", "scikit-learn", "pandas"],
    extras_require={"dev": ["dagit", "pytest", "isort", "black", "ruff"]},
)
