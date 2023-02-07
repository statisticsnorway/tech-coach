import importlib
import os
from pathlib import Path


def repo_root_dir(directory: Path = Path.cwd()) -> Path | None:
    """Find the root directory of a git repo, searching upwards from a given path.

    Args:
        directory: The path to search from, defaults to the current working directory.

    Returns:
        Path to the git repo's root directory or None if not found.

    """
    while directory / ".git" not in directory.iterdir():
        if directory == Path("/"):
            return None
        else:
            directory = directory.parent
    return directory


def run_python_script_inline(filename: str) -> None:
    """Run an external python script inline in the current process.

    Args:
        filename: Filename of the script to run, including path.

    """
    assert Path(filename).is_file(), f"File {filename} not found"

    spec = importlib.util.spec_from_file_location("myscript", filename)
    module = importlib.util.module_from_spec(spec)  # type: ignore
    spec.loader.exec_module(module)  # type: ignore


def use_virtualenv_in_pyspark() -> None:
    """Use a poetry generated virtual environment with distributed pyspark.

    When using a custom virtual environment with pyspark and multiple worker nodes,
    the virtual environment must be copied to each worker node. This function takes
    care of this.

    Precondition: The virtual environment to distribute must exist in the root directory
    as a file named pyspark_venv.tar.gz. It can be created with the command:
    venv-pack -p .venv -o pyspark_venv.tar.gz

    The code in the function is based on https://manual.dapla.ssb.no/pyspark-venv.html

    """
    pack_file = repo_root_dir() / "pyspark_venv.tar.gz"
    assert pack_file.is_file(), "File pyspark_venv.tar.gz not found in root directory"

    # Environment variable pointing to an unpacked version of the virtual environment
    os.environ["PYSPARK_PYTHON"] = "./environment/bin/python"

    # Add a flag, --archives, pointing to archive with the virtual environment as the
    # last element in PYSPARK_K8S_SUBMIT_ARGS
    if "PYSPARK_K8S_SUBMIT_ARGS" in os.environ:
        conf = os.environ["PYSPARK_K8S_SUBMIT_ARGS"].split(" ")
        last_index = conf.index("pyspark-shell")
        conf[last_index:last_index] = ["--archives", f"{str(pack_file)}#environment"]
        os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join(conf)

    script_name = "/usr/local/share/jupyter/kernels/pyspark_k8s/init.py"
    run_python_script_inline(script_name)
