from setuptools import setup, find_packages

setup(
    name="ebrains-dataproxy-sync",
    version="0.0.1",
    author="Xiao Gui",
    author_email="xgui3783@gmail.com",
    description="Sync local directory to ebrains dataproxy",
    packages=find_packages(include=["ebrains_dataproxy_sync", "ebrains_dataproxy_sync.*"]),
    python_requires=">=3.7",
    install_requires=[
        "requests",
        "tqdm",

        # waiting for
        # https://github.com/HumanBrainProject/ebrains-drive/pull/27
        # https://github.com/HumanBrainProject/ebrains-drive/pull/31
        # to merge and release
        # once merged, use ebrains-drive as dependency
        # PR is merged on master, but not yet released. use from pypi once released
        "ebrains_drive @ git+https://github.com/HumanBrainProject/ebrains-storage.git@fa59fe12d22eb252b0c15a7c4772ab4b27a62452",
    ]
)
