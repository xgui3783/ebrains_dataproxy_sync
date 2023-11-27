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
        # to merge and release
        # once merged, use ebrains-drive as dependency
        "ebrains_drive @ git+https://github.com/xgui3783/ebrains-drive.git@tmp_customhdr",
    ]
)
