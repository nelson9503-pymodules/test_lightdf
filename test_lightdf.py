import os
import lightdf
import unittest


class Testlightdf(unittest.TestCase):

    def setUp(self):
        # set test csv
        csvtext = """h1,h2,h3
1,2,3
4,5,6
7,8,9
"""
        with open("test.csv", 'w') as f:
            f.write(csvtext)
        # set default csv
        self.defaultdf = lightdf.new(
            datalist=[[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            indexlist=["r1", "r2", "r3"],
            headerlist=["h1", "h2", "h3"]
        )

    def test_new(self):
        # empty
        df = lightdf.new()
        # with datalist
        df = lightdf.new(
            datalist=[[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        )
        assert df.data(0, 0) == 1
        assert df.data("1", "1") == 5
        # with indexlist
        df = lightdf.new(
            indexlist=["r1", "r2", "r3"]
        )
        assert df.list_index()[1] == "r2"
        # with headerlist
        df = lightdf.new(
            headerlist=["c1", "c2", "c3"]
        )
        assert df.list_header()[1] == "c2"
        # with all arguments
        df = lightdf.new(
            datalist=[[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            indexlist=["r1", "r2", "r3"],
            headerlist=["c1", "c2", "c3"]
        )
        assert df.data(0, 0) == 1
        assert df.data("r2", 1) == 5
        assert df.data(2, "c3") == 9

    def test_readcsv(self):
        # without headers
        df = lightdf.read_csv("test.csv", False)
        assert df.data(0, 0) == "h1"
        assert int(df.data("1", "1")) == 2
        # with headers
        df = lightdf.read_csv("test.csv", True)
        assert int(df.data(0, 0)) == 1
        assert int(df.data("1", "h2")) == 5

    def test_readdict(self):
        df = lightdf.read_dict({
            "r1": {"h1": 1, "h2": 2, "h3": 3},
            "r2": {"h1": 4, "h2": 5, "h3": 6},
            "r3": {"h1": 7, "h2": 8, "h3": 9}
        })
        assert df.data(0, 0) == 1
        assert df.data("r2", "h2") == 5

    def test_writedata(self):
        df = self.defaultdf
        df.wdata(0, 0, "teststring")
        assert df.data(0, 0) == "teststring"
        df.wdata("r2", 1, True)
        assert df.data(1, 1) == True
        df.wdata(2, "h3", 123.123)
        assert df.data(2, 2) == 123.123

    def test_indexControl(self):
        df = self.defaultdf
        indexList = df.list_index()
        assert indexList == ["r1", "r2", "r3"]
        df.add_index("r4")
        assert df.list_index()[3] == "r4"
        df.wdata("r4", 0, "test")
        assert df.data("r4", 0) == "test"
        df.drop_index("r4")
        assert df.list_index() == ["r1", "r2", "r3"]
        df.add_index()
        assert df.list_index()[3] == "3"
        df.wdata(3, 0, "test")
        assert df.data(3, 0) == "test"
        df.drop_index(3)
        assert df.list_index() == ["r1", "r2", "r3"]

    def test_headerControl(self):
        df = self.defaultdf
        headerList = df.list_header()
        assert headerList == ["h1", "h2", "h3"]
        df.add_header("h4")
        assert df.list_header()[3] == "h4"
        df.wdata(0, "h4", "test")
        assert df.data(0, 3) == "test"
        df.drop_header("h4")
        assert df.list_header() == ["h1", "h2", "h3"]
        df.add_header()
        assert df.list_header()[3] == "3"
        df.wdata(0, 3, "test")
        assert df.data(0, 3) == "test"
        df.drop_header(3)
        assert df.list_header() == ["h1", "h2", "h3"]

    def test_to_list(self):
        df = self.defaultdf
        li = df.to_list(False, False)
        assert li == [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        li = df.to_list(True, False)
        assert li == [["r1", 1, 2, 3], ["r2", 4, 5, 6], ["r3", 7, 8, 9]]
        li = df.to_list(False, True)
        assert li == [["h1", "h2", "h3"], [1, 2, 3], [4, 5, 6], [7, 8, 9]]
        li = df.to_list(True, True)
        assert li == [["Index", "h1", "h2", "h3"], [
            "r1", 1, 2, 3], ["r2", 4, 5, 6], ["r3", 7, 8, 9]]

    def test_to_dict(self):
        df = self.defaultdf
        d = df.to_dict()
        assert d == {
            "r1": {"h1": 1, "h2": 2, "h3": 3},
            "r2": {"h1": 4, "h2": 5, "h3": 6},
            "r3": {"h1": 7, "h2": 8, "h3": 9}
        }

    def test_to_csv(self):
        df = self.defaultdf
        df.to_csv("testtocsv.csv", False, False)
        check = lightdf.read_csv("testtocsv.csv", False)
        assert int(check.data(0, 0)) == 1
        assert int(check.data(1, 1)) == 5
        df.to_csv("testtocsv.csv", False, True)
        check = lightdf.read_csv("testtocsv.csv", True)
        assert int(check.data(0, "h1")) == 1
        assert int(check.data(1, "h2")) == 5
        df.to_csv("testtocsv.csv", True, False)
        check = lightdf.read_csv("testtocsv.csv", False)
        assert check.data(0, 0) == "r1"
        os.remove("testtocsv.csv")

    def tearDown(self):
        os.remove("test.csv")


if __name__ == "__main__":
    unittest.main()
