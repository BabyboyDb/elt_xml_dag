from tokenize import group
from typing import Dict
from dagster import asset
from scripts import xml_lib, db_lib

import logging
import pandas as pd

from datetime import datetime
import xml.etree.ElementTree as ET
import os




# Параметры соединения с базой данных
param_dic = {
    "host"      : "192.168.1.50",
    "dbname"    : "gsmk",
    "user"      : "postgres",
    "password"  : "postgres"
}

# задаем каталог источника и архива данных (xml)
workdir = 'IN'
outdir = 'OUT'


main_param = {}
main_param['src_files'] = []
main_param['src_data'] = {}

@asset(group_name="LoadXML")
def source_data_parameters() -> Dict:
    # задаем словарь массив файлов источников и их параметры
    src_data={}


    # параметры для данных из файла onk_cases.xml

    src_files=['onk_cases.xml']
    xml_findkey = 'SL'
    xml_db_tables = ['onk_sl','b_diag','onk_usl','lek_pr']
    db_tables_keys =  ['id_case','id_case','id_service_onk','id_service_onk']
    src_data['onk_cases.xml']=[xml_findkey, xml_db_tables, db_tables_keys]

    # параметры для данных из файла onk_ds_cases.xml

    src_files.append('onk_ds_cases.xml')
    xml_findkey = 'case_ds_onk'
    xml_db_tables = ['onk_ds_cases']
    db_tables_keys =  ['id_case']
    src_data['onk_ds_cases.xml']=[xml_findkey, xml_db_tables, db_tables_keys]

    # параметры для данных из файла casedirs.xml

    src_files.append('casedirs.xml')
    xml_findkey = 'case_direction'
    xml_db_tables = ['casedirs']
    db_tables_keys =  ['id_case_direction']
    src_data['casedirs.xml']=[xml_findkey, xml_db_tables, db_tables_keys]

    # параметры для данных из файла caseadd.xml

    src_files.append('caseadd.xml')
    xml_findkey = 'case_add'
    xml_db_tables = ['caseadd']
    db_tables_keys =  ['id_case_add']
    src_data['caseadd.xml']=[xml_findkey, xml_db_tables, db_tables_keys]

    # параметры для данных из файла visits.xml

    src_files.append('visits.xml')
    xml_findkey = 'visit'
    xml_db_tables = ['visits']
    db_tables_keys =  ['id_visit']
    src_data['visits.xml']=[xml_findkey, xml_db_tables, db_tables_keys]

    # параметры для данных из файла cards.xml

    src_files.append('cards.xml')
    xml_findkey = 'infis_card'
    xml_db_tables = ['cards']
    db_tables_keys =  ['id_service']
    src_data['cards.xml']=[xml_findkey, xml_db_tables, db_tables_keys]

    main_param['src_files'] = src_files
    main_param['src_data'] = src_data
    return main_param

# extract data from xml
@asset(group_name="LoadXML")
def extract_from_xml_file(source_data_parameters)-> Dict:
    # Соединяемся с базой данных
    conn = db_lib.connect(param_dic)
    conn.set_client_encoding('UTF8')

    main_param = source_data_parameters

    src_files = main_param['src_files']
    src_data = main_param['src_data']
    #main_param = {}
    main_param['connection'] = conn

    """ проходим по каждому источику *(файлу xml),
        формируем список таблиц загрузки и ключей уникальности
        вызываем процедуру загрузки для каждой таблицы
    """

    for file in src_files:

        src_file = os.getcwd()+'/'+ workdir +'/'+file

        if not os.path.exists(src_file):
            continue

        for db_table in src_data[file][1]:
            ind = src_data[file][1].index(db_table)
            print(f"файл  {file} --- таблица {db_table} ---")

            # Формируем список колонок таблицы бд
            tbl_cols = db_lib.get_columns_names(conn, db_table)
            # Формируем словарь размеров полей таблицы бд (для выравнивания, в xml размер завышен)
            tbl_cols_name_size = db_lib.get_tbl_columns_name_and_size(conn, db_table)
            fldsizes={}
            for name, size in tbl_cols_name_size:
                fldsizes[name]=size

            # Формируем список колонок таблицы бд типов integer, numeric, smallint, date

            fldtypes = {}
            fldtypes['integer'] = db_lib.get_tbl_columns_names_only_type(conn, db_table, "integer")
            fldtypes['numeric'] = db_lib.get_tbl_columns_names_only_type(conn, db_table, "numeric")
            fldtypes['smallint'] = db_lib.get_tbl_columns_names_only_type(conn, db_table, "smallint")
            fldtypes['date'] = db_lib.get_tbl_columns_names_only_type(conn, db_table, "date")
            fldtypes['float'] = db_lib.get_tbl_columns_names_only_type(conn, db_table, "double precision")
            fldtypes['string'] = db_lib.get_tbl_columns_names_only_type(conn, db_table, "character varying")


            # Формируем словарь основных параметров вызова процедур

            main_param['table'] = db_table
            main_param['table_columns'] = tbl_cols
            main_param['max_records'] = 1000
            main_param['xml_file'] = src_file
            main_param['xml_findkey'] = src_data[file][0]
            main_param['tbl_fldtypes'] = fldtypes
            main_param['unique_key'] = src_data[file][2][ind]
            main_param['column_sizes'] = fldsizes

            # Удаляем записи в таблице
            db_lib.truncate_table(main_param['connection'], main_param['table'])

            ### Запуск основной процедуры загрузки даных

            start_time = datetime.now()
            print(f"Время старта --- {datetime.now()} ---")
            xml_lib.main_process_xml(main_param=main_param)
            print(f"Время завершения --- {datetime.now()} ---")
            print("--- %s seconds ---" % (datetime.now() - start_time))


    return main_param
