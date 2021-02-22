import os
import lightdf
import unittest


class Testlightdf(unittest.TestCase):

    def setUp(self):
        # create base dataframe
        df = lightdf.Dataframe("id", int)
        df.add_col("name", str)
        df.add_col("gender", str)

    def test_read_write_delete(self):
        df = lightdf.Dataframe("id", int)
        df.add_col("name", str)
        df.add_col("gender", str)
        df.write(1, "name", "Nelson")
        df.write(1, "gender", None)
        df.write(2, "name", "John")
        df.write(2, "gender", "M")
        d = df.read(1, "name")
        assert d == "Nelson", "Read Failed."
        d = df.read(1, "gender")
        assert d == None, "Read Failed."
        df.delete(2)
        keys = df.list_keys()
        assert keys == [1], "Delete Row Failed."

    def test_drop_col(self):
        df = lightdf.Dataframe("id", int)
        df.add_col("name", str)
        df.add_col("gender", str)
        df.drop_col("name")
        cols = df.list_col()
        assert cols == ["gender"], "Drop Col Failed."

    def test_change_column_type(self):
        df = lightdf.Dataframe("id", int)
        df.add_col("name", str)
        df.add_col("gender", str)
        df.write(1, "name", "1")
        df.write(2, "name", "2")
        df.set_col_type("name", int)
        types = df.list_col_type()
        assert types == [int, str], "Change column Failed."

    def test_change_key_cols(self):
        df = lightdf.Dataframe("id", int)
        df.add_col("name", str)
        df.add_col("gender", str)
        df.write(1, "name", "Nelson")
        df.write(1, "gender", "M")
        df.write(2, "name", "John")
        df.write(2, "gender", "M")
        df.set_keys_column("name")
        li = df.list_keys()
        assert li == ["Nelson", "John"], "Change Keys Failed."
        li = df.list_col()
        assert li == ["gender", "id"], "Change Keys Failed."

    def test_to_from_csv(self):
        df = lightdf.Dataframe("id", int)
        df.add_col("name", str)
        df.add_col("gender", str)
        df.write(1, "name", "Nelson")
        df.write(1, "gender", "M")
        df.write(2, "name", "John")
        df.write(2, "gender", "M")
        df.to_csv("test.csv")
        df = lightdf.Dataframe("id", int)
        df.add_col("name", str)
        df.add_col("gender", str)
        df.from_csv("test.csv")
        d = df.read(1, "name")
        assert d == "Nelson", "Read CSV Failed."
        d = df.read(2, "name")
        assert d == "John", "Read CSV Failed."
        os.remove("test.csv")

    def test_to_from_dict(self):
        df = lightdf.Dataframe("id", int)
        df.add_col("name", str)
        df.add_col("gender", str)
        df.write(1, "name", "Nelson")
        df.write(1, "gender", "M")
        df.write(2, "name", "John")
        df.write(2, "gender", "M")
        dictionary = df.to_dict()
        d = dictionary[1]["name"]
        assert d == "Nelson", "To Dict Failed."
        df = lightdf.Dataframe("id", int)
        df.add_col("name", str)
        df.add_col("gender", str)
        df.from_dict(dictionary)
        d = df.read(1, "name")
        assert d == "Nelson", "From Dict Failed."

    def test_from_dataframe(self):
        df = lightdf.Dataframe("id", int)
        df.add_col("name", str)
        df.add_col("gender", str)
        df.write(1, "name", "Nelson")
        df.write(1, "gender", "M")
        df.write(2, "name", "John")
        df.write(2, "gender", "M")
        df2 = lightdf.Dataframe("id", int)
        df2.add_col("name", str)
        df2.add_col("gender", str)
        df2.write(1, "name", "NelsonLai")
        df2.write(1, "gender", "M")
        df2.write(3, "name", "Mary")
        df2.write(3, "gender", "F")
        df.join_dataframe(df2)

    def tearDown(self):
        if os.path.exists("test.csv"):
            os.remove("test.csv")


if __name__ == "__main__":
    unittest.main()
