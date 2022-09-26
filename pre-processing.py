from abc import ABC, abstractmethod
from cmath import nan
import pandas as pd
import os
from dataset_name import DatasetName
import math


class Builder(ABC):
    """definisce la classe astratta builder"""

    @abstractmethod
    def build(self): pass
    @abstractmethod
    def reset(self): pass


class CleanData:
    """classe per fare il pre-processing dei dati"""

    path = None
    time_table_name = None
    __max_row_length = 100000

    def __write_time_to_csv(self, columns, rows):
        time_table_columns = columns + ['Year', 'Value']
        time_table_output = os.path.join(
            self.path, self.time_table_name)

        time_table = pd.DataFrame(
            rows, columns=time_table_columns)

        with open(time_table_output, 'a', encoding='utf8', newline='\n') as file:
            time_table.to_csv(
                file, header=file.tell() == 0, index=False, sep=",")

    def generate_time_table(self):

        # otteniamo la lista delle colonne degli anni
        data_rows = pd.read_csv(os.path.join(
            self.path, DatasetName.data.value))

        data_columns = data_rows.columns

        data_column_names = list(data_columns.values)

        data_years = list()
        string_columns = list()

        # estraiamo la lista delle colonne "Anno"
        # es. 1960,1961...
        for data_column_name in data_column_names:
            tuple = [data_column_name, data_columns.get_loc(data_column_name)]
            if("Unnamed: " in data_column_name):
                continue
            if data_column_name.isnumeric():
                data_years.append(
                    tuple)
            else:
                string_columns.append(tuple)

        new_table_rows = list()
        new_table_columns = [string_column[0]
                             for string_column in string_columns]
    

        for data_row in data_rows.itertuples(index=False):

            for year in data_years:

                value = data_row[year[1]]

                # scriviamo una riga sulla nuova tabella del tempo
                if not math.isnan(value):
                    new_row = list()
                    for string_column in string_columns:
                        new_row.append(data_row[string_column[1]])

                    new_row.append(int(year[0]))
                    new_row.append(value)

                    new_table_rows.append(new_row)

                    # per evitare sovraccarichi di memoria centrale
                    if(len(new_table_rows) == self.__max_row_length):
                        self.__write_time_to_csv(
                            new_table_columns, new_table_rows)
                        new_table_rows.clear()
                        print("csv tempo aggiornato")

        self.__write_time_to_csv(
            new_table_columns, new_table_rows)
        print("csv tempo caricato")


class CleanDataBuilder(Builder):
    """ builder per gestire la creazione di CleanData"""

    __path = "data"
    __output_time_table = "WDIData_New.csv"
    __clean_data = None

    def __init__(self):
        self.reset()

    # definisce path con tutti i csv del dataset
    def set_path(self, path):
        if path is not None:
            self.__clean_data.path = path

        return self

    # definisce path con tutti i csv del dataset
    def set_time_table(self, name):
        if name is not None:
            self.__clean_data.time_table_name = name

        return self

    # costruisce clean_data
    def build(self):
        element = self.__clean_data
        self.reset()
        return element

    # pulisce l'oggetto dopo averlo costruito
    def reset(self):
        self.__clean_data = CleanData()
        self.__clean_data.path = self.__path
        self.__clean_data.time_table_name = self.__output_time_table


if __name__ == "__main__":
    clean_data = CleanDataBuilder().build()
    clean_data.generate_time_table()
