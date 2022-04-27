import itertools
import sys
import pandas as pd
import requests
import re
from datetime import datetime, timedelta
import patoolib
import shutil
import glob
import os
import csv
from datetime import date, timedelta

class exceso_mortalidad:
    def __init__(self, url, output):


        # store used variables
        self.url = url
        self.output = output

        # init stuff

        aux = self.url.split('/')
        self.file_name = aux[len(aux) - 1]

        self.date = re.search("\d{8}", self.file_name).group(0)
        date = datetime.strptime(self.date, "%d%m%Y") + timedelta(days=1)
        self.date = datetime.strftime(date, "%Y-%m-%d")





        self.df_confirmados = None
        self.df_sospechosos = None

    def get_last_file(self):
        response = requests.get(self.url)
        if (response.reason == 'Not Found'):
            self.nuevo = False
        else:
            rar_path = '../input/DEIS/' + self.file_name
            with open(rar_path, 'wb') as f:
                f.write(response.content)
            # unrar('tmp.rar')
            patoolib.extract_archive(rar_path, outdir='../input/DEIS')

            # move the file of interest from tmp to input, nuke tmp
            csv_tmp_path = '../input/DEIS/' + self.file_name.replace('zip', 'csv')
            csv_path = '../input/DEIS/' + self.file_name.replace('zip', 'csv')
            shutil.move(csv_tmp_path, csv_path)

            for file in glob.glob('../input/DEIS/*'):
                if '.csv' not in file:
                    print('removing ' + file)
                    os.remove(file)
            self.nuevo = True

    def last_file_to_pd(self):
        csv_path = '../input/DEIS/' + self.file_name.replace('zip', 'csv')

        # find separator:
        # load the first few lines, to guess the CSV dialect
        head = ''.join(itertools.islice(open(csv_path, encoding='ISO-8859-1'), 5))
        s = csv.Sniffer()
        my_separator = s.sniff(head).delimiter
        print('Found separator: ' + my_separator)

        df = pd.read_csv(csv_path, sep=my_separator, encoding='ISO-8859-1', header=None)
        os.remove(csv_path)
        #CONSTRUIR COLUMNAS QUE SERVIRAN PARA CALCULAR DEFUNCIONES ESPERADAS, CONFIRMADAS Y SOSPECHOSAS POR COVID, EXCESO DE MORTALIDAD, DIFERENCIA

        cols = df.columns.to_list()
        df['confirmadas_2020'] = 0
        df['confirmadas_2021'] = 0
        df['confirmadas_2022'] = 0
        df['sospechosas_2020'] = 0
        df['sospechosas_2021'] = 0
        df['sospechosas_2022'] = 0
        df['defunciones_2016'] = 0
        df['defunciones_2017'] = 0
        df['defunciones_2018'] = 0
        df['defunciones_2019'] = 0
        df['defunciones_2020'] = 0
        df['defunciones_2021'] = 0
        df['defunciones_2022'] = 0

        df['date'] = pd.to_datetime(df[cols[1]], format='%Y-%m-%d')
        df['week'] = df['date'].dt.strftime('%U')
        df['year'] = df['date'].dt.strftime('%Y')
        df['year-week'] = df['date'].dt.strftime('%Y') + '-' + df['date'].dt.strftime('%U')

        #BORRAR FILAS DE SEMANAS RARAS
        df = df.loc[df['week'] != '00']
        df = df.loc[df['week'] != '53']

        #FILTRAR
        df.loc[df['year'] == '2016', ['defunciones_2016']] = 1
        df.loc[df['year'] == '2017', ['defunciones_2017']] = 1
        df.loc[df['year'] == '2018', ['defunciones_2018']] = 1
        df.loc[df['year'] == '2019', ['defunciones_2019']] = 1
        df.loc[df['year'] == '2020', ['defunciones_2020']] = 1
        df.loc[df['year'] == '2021', ['defunciones_2021']] = 1
        df.loc[df['year'] == '2022', ['defunciones_2022']] = 1
        df.loc[(df['year'] == '2020') & (df[cols[8]] == 'U071'), ['confirmadas_2020']] = 1
        df.loc[(df['year'] == '2021') & (df[cols[8]] == 'U071'), ['confirmadas_2021']] = 1
        df.loc[(df['year'] == '2022') & (df[cols[8]] == 'U071'), ['confirmadas_2022']] = 1
        df.loc[(df['year'] == '2020') & (df[cols[8]] == 'U072'), ['sospechosas_2020']] = 1
        df.loc[(df['year'] == '2021') & (df[cols[8]] == 'U072'), ['sospechosas_2021']] = 1
        df.loc[(df['year'] == '2022') & (df[cols[8]] == 'U072'), ['sospechosas_2022']] = 1

        #CONTAR POR ANO
        df_defunciones_agrupado_2016 = df.groupby(['week'])['defunciones_2016'].sum().reset_index()
        df_defunciones_agrupado_2017 = df.groupby(['week'])['defunciones_2017'].sum().reset_index()
        df_defunciones_agrupado_2018 = df.groupby(['week'])['defunciones_2018'].sum().reset_index()
        df_defunciones_agrupado_2019 = df.groupby(['week'])['defunciones_2019'].sum().reset_index()
        df_defunciones_agrupado_2020 = df.groupby(['week'])['defunciones_2020'].sum().reset_index()
        df_defunciones_agrupado_2021 = df.groupby(['week'])['defunciones_2021'].sum().reset_index()
        df_defunciones_agrupado_2022 = df.groupby(['week'])['defunciones_2022'].sum().reset_index()
        df_confirmadas_agrupado_2020 = df.groupby(['week'])['confirmadas_2020'].sum().reset_index()
        df_confirmadas_agrupado_2021 = df.groupby(['week'])['confirmadas_2021'].sum().reset_index()
        df_confirmadas_agrupado_2022 = df.groupby(['week'])['confirmadas_2022'].sum().reset_index()
        df_sospechosas_agrupado_2020 = df.groupby(['week'])['sospechosas_2020'].sum().reset_index()
        df_sospechosas_agrupado_2021 = df.groupby(['week'])['sospechosas_2021'].sum().reset_index()
        df_sospechosas_agrupado_2022 = df.groupby(['week'])['sospechosas_2022'].sum().reset_index()
        df_2016_2017 = pd.merge_ordered(df_defunciones_agrupado_2016, df_defunciones_agrupado_2017, on='week')
        df_2016_2018 = pd.merge_ordered(df_2016_2017, df_defunciones_agrupado_2018, on='week')
        df_2016_2019 = pd.merge_ordered(df_2016_2018, df_defunciones_agrupado_2019, on='week')
        df_2016_2020 = pd.merge_ordered(df_2016_2019, df_defunciones_agrupado_2020, on='week')
        df_2016_2021 = pd.merge_ordered(df_2016_2020, df_defunciones_agrupado_2021, on='week')
        df_2016_2022 = pd.merge_ordered(df_2016_2021, df_defunciones_agrupado_2022, on='week')
        df_2016_2020_c = pd.merge_ordered(df_2016_2022, df_confirmadas_agrupado_2020, on='week')
        df_2016_2021_c = pd.merge_ordered(df_2016_2020_c, df_confirmadas_agrupado_2021, on='week')
        df_2016_2022_c = pd.merge_ordered(df_2016_2021_c, df_confirmadas_agrupado_2022, on='week')
        df_2016_2020_s = pd.merge_ordered(df_2016_2022_c, df_sospechosas_agrupado_2020, on='week')
        df_2016_2021_s = pd.merge_ordered(df_2016_2020_s, df_sospechosas_agrupado_2021, on='week')
        df_2016_2022_s = pd.merge_ordered(df_2016_2021_s, df_sospechosas_agrupado_2022, on='week')

        #CALCULO DE TOTALES Y PROMEDIOS
        df_2016_2022_s['defunciones_covid_2020'] = df_2016_2022_s.fillna(0)['confirmadas_2020'] + df_2016_2022_s.fillna(0)['sospechosas_2020']
        df_2016_2022_s['defunciones_covid_2021'] = df_2016_2022_s.fillna(0)['confirmadas_2021'] + \
                                                   df_2016_2022_s.fillna(0)['sospechosas_2021']
        df_2016_2022_s['defunciones_covid_2022'] = df_2016_2022_s.fillna(0)['confirmadas_2021'] + \
                                                   df_2016_2022_s.fillna(0)['sospechosas_2022']
        df_2016_2022_s['promedio_2016_2019'] = (df_2016_2022_s['defunciones_2016'] + df_2016_2022_s['defunciones_2017'] + df_2016_2022_s['defunciones_2018'] + df_2016_2022_s['defunciones_2019'])/4

        #SEPARAR POR ANO
        df_exceso_mortalidad_2020 = df_2016_2022_s[['defunciones_covid_2020','defunciones_2020','promedio_2016_2019']]
        df_exceso_mortalidad_2020['resta_def_prom'] = df_2016_2022_s['defunciones_2020'] - df_2016_2022_s['promedio_2016_2019']
        df_exceso_mortalidad_2020.reset_index()
        df_exceso_mortalidad_2020.rename(columns={'index':'week'})
        df_exceso_mortalidad_2020['week'] = df_exceso_mortalidad_2020.index + 1
        df_exceso_mortalidad_2020 = df_exceso_mortalidad_2020[
            ['week','defunciones_2020', 'promedio_2016_2019','resta_def_prom','defunciones_covid_2020']]

        df_exceso_mortalidad_2021 = df_2016_2022_s[['defunciones_covid_2021','defunciones_2021','promedio_2016_2019']]
        df_exceso_mortalidad_2021['resta_def_prom'] = df_2016_2022_s['defunciones_2021'] - df_2016_2022_s['promedio_2016_2019']
        df_exceso_mortalidad_2021.reset_index()
        df_exceso_mortalidad_2021.rename(columns={'index': 'week'})
        df_exceso_mortalidad_2021['week'] = df_exceso_mortalidad_2021.index + 1
        df_exceso_mortalidad_2021 = df_exceso_mortalidad_2021[
            ['week', 'defunciones_2021', 'promedio_2016_2019', 'resta_def_prom', 'defunciones_covid_2021']]


        df_exceso_mortalidad_2022 = df_2016_2022_s[['defunciones_covid_2022', 'defunciones_2022', 'promedio_2016_2019']]
        df_exceso_mortalidad_2022['resta_def_prom'] = df_2016_2022_s['defunciones_2022'] - df_2016_2022_s['promedio_2016_2019']
        df_exceso_mortalidad_2022.reset_index()
        df_exceso_mortalidad_2022.rename(columns={'index': 'week'})
        df_exceso_mortalidad_2022['week'] = df_exceso_mortalidad_2022.index + 1
        df_exceso_mortalidad_2022 = df_exceso_mortalidad_2022[
            ['week', 'defunciones_2022', 'promedio_2016_2019', 'resta_def_prom', 'defunciones_covid_2022']]
        df_exceso_mortalidad_2022 = df_exceso_mortalidad_2022.loc[df_exceso_mortalidad_2022['defunciones_2022'] != 0]



        #ESCRIBIR
        df_exceso_mortalidad_2020.to_csv('../output/producto1/df_deis_exceso_mortalidad_2020.csv', encoding='utf-8-sig', index=False)
        df_exceso_mortalidad_2021.to_csv('../output/producto1/df_deis_exceso_mortalidad_2021.csv', encoding='utf-8-sig', index=False)
        df_exceso_mortalidad_2022.to_csv('../output/producto1/df_deis_exceso_mortalidad_2022.csv', encoding='utf-8-sig', index=False)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Identificamos el último archivo")
        now = date.today() - timedelta(days=13)
        my_url = 'https://repositoriodeis.minsal.cl/DatosAbiertos/VITALES/DEFUNCIONES_FUENTE_DEIS_2016_' + now.strftime("%Y") + '_' + now.strftime("%d%m%Y") + '.zip'
        my_prod = exceso_mortalidad(my_url, '../output/producto1/exceso_mortalidad_DEIS.csv')
        my_prod.get_last_file()
        if(my_prod.nuevo):
            print("Hacemos el workflow para los archivos del DEIS para generar el análisis de exceso de mortalidad")

            my_prod.last_file_to_pd()
        else: print("no new files in DEIS")

    if len(sys.argv) == 2:
        print("Identificamos el último archivo")
        my_days_offset = int(sys.argv[1])
        now = date.today() - timedelta(days=my_days_offset)
        my_url = 'https://repositoriodeis.minsal.cl/DatosAbiertos/VITALES/DEFUNCIONES_FUENTE_DEIS_2016_' + now.strftime(
            "%Y") + '_' + now.strftime("%d%m%Y") + '.zip'
        my_prod = exceso_mortalidad(my_url, '../output/producto1/exceso_mortalidad_DEIS.csv')
        my_prod.get_last_file()
        if (my_prod.nuevo):
            print("Hacemos el workflow para los archivos del DEIS para generar p97")

            my_prod.last_file_to_pd()
        else:
            print("no new files in DEIS")