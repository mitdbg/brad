class S3Path:
    def __init__(self, path_with_file: str) -> None:
        self._path_prefix = "/".join(path_with_file.split("/")[:-1]) + "/"
        self._path_with_file = path_with_file

    def path_prefix(self) -> str:
        return self._path_prefix

    def path_with_file(self) -> str:
        return self._path_with_file
