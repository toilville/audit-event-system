from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="toilville-audit-events",
    version="1.0.0",
    author="Toilville LLC",
    author_email="engineering@toilville.com",
    description="Purview-compatible audit event consumer for Kafka→PostgreSQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/toilville/audit-event-system",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: System :: Logging",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.11",
    install_requires=[
        "kafka-python>=2.0.2",
        "psycopg2-binary>=2.9.9",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "pytest-asyncio>=0.21.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "audit-consumer=audit_events.daemon:main",
            "emit-audit-event=audit_events.cli:emit_event",
        ],
    },
)
