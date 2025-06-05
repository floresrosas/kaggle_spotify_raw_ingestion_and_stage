import unittest
import chatgpt_pipeline.raw_ingestion as p
from pathlib import Path



class MyTests(unittest.TestCase):
    def _recursive_dir_delete(self, path: Path):
        while path != path.parent:
            try:
                path.rmdir()
                path = path.parent
            except OSError as e:
                print(f"Directory is not empty: {e}")
    
    def test_create_path_directories(self):
        test_input = "this/is/a/test/path/example.txt"
        test_output = "this/is/a/test/path"
        p.create_path_directories(test_input)
        res = Path(test_output)
        assert(res.exists())
        self._recursive_dir_delete(res)

    def test_open_zip_file(self):
        zip_path = 'chatgpt_pipeline/tests/artifacts/test.zip'
        if not Path(zip_path).exists():
            raise Exception(f"Missing test input {zip_path}")

        download_path = 'output/test/data/unzip/'
        p.open_zip_file(zip_path, download_path)

        res = Path(download_path+'Asta')
        assert(res.exists())
        
        for file_path in res.iterdir():
            if file_path.is_file():
                file_path.unlink()
        self._recursive_dir_delete(res)

        



suite = MyTests()
suite.test_create_path_directories()
suite.test_open_zip_file()
        

