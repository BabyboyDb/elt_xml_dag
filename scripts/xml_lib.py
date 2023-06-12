import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
from . import db_lib
import datetime
import re

# Функции преобразования типов


def format_dates(df, columns):
    for column in columns:
        if str(df[column].dtype)!='datetime64[ns]':
            df[column] = df[column].str.replace('-', '.')
            df[column] = df[column].str.replace('/', '.')
            df[column] = pd.to_datetime(df[column], errors="coerce", format="%d.%m.%Y")


def format_float(df, columns):
    for column in columns:
        if str(df[column].dtype) != 'float64':
            if str(df[column].dtype) == 'object':
                df[column] = df[column].str.replace(',', '.')
            df[column] = df[column].fillna(0.0).astype(np.float64, errors='ignore')


def format_numeric(df, columns):
    for column in columns:
        if str(df[column].dtype) != 'float64':
            if str(df[column].dtype) == 'object':
                df[column] = df[column].str.replace(',', '.')
            df[column] = pd.to_numeric(df[column])
            df[column] = df[column].fillna(0.0).astype(np.float64, errors='ignore')


def format_int(df, columns):
    for column in columns:
        if str(df[column].dtype)!='int32':
            df[column] = df[column].fillna(0).astype(np.int32, errors="ignore")


def format_s_int(df, columns):
    for column in columns:
        if str(df[column].dtype) != 'int8':
            df[column] = df[column].fillna(0).astype(np.int8)


def format_bool(df, columns):
    if columns:
        df[columns] = df[columns].apply(lambda x: np.where(x == 'T', True, False))


def format_str(df, fldsizes, columns):
    for column in columns:
        if str(df[column].dtype) == 'object':
            df[column] = df[column].fillna('').apply(lambda x: x[:fldsizes[column]])
#            df[column] = df[column].fillna('').apply(str)



# Функция возвращает генератор полей записи в наборе xml
# Выбираем только те поля, которые есть в списке, передаваемом параметром tbl_cols

def add_elem2dict(fld, tbl_cols, r_dict):
    if fld.tag.lower() in tbl_cols:
        key = fld.tag.lower()
        r_dict.setdefault(key, []).append(fld.text)


def add_empty_row2dict(r_dict, table_columns):
    for col_name in table_columns:
        r_dict.setdefault(col_name, []).append(None)


def change_elem2dict(fld, tbl_cols, r_dict):
    if fld.tag.lower() in tbl_cols:
        key = fld.tag.lower()
        r_dict[key][-1] = fld.text




def prepare_df(part_dict, main_param):
    df = pd.DataFrame.from_dict(part_dict)

# удаляем пустые колонки
    df = df.dropna(axis=1, how='all')

# Формируем список колонок фрейма
    df_cols = list(df.columns)

    # Формируем список колонок фрейма типов integer, numeric, smallint, date
    columns4int = db_lib.get_list_columns4type(df_cols, main_param['tbl_fldtypes']['integer'])
    columns4num = db_lib.get_list_columns4type(df_cols, main_param['tbl_fldtypes']['numeric'])
    columns4s_int = db_lib.get_list_columns4type(df_cols, main_param['tbl_fldtypes']['smallint'])
    columns4date = db_lib.get_list_columns4type(df_cols, main_param['tbl_fldtypes']['date'])
    columns4float = db_lib.get_list_columns4type(df_cols, main_param['tbl_fldtypes']['float'])
    columns4str = db_lib.get_list_columns4type(df_cols, main_param['tbl_fldtypes']['string'])

    # Формируем типы в df
    format_dates(df, columns4date)
    # format="%d/%m/%Y"

    format_int(df, columns4int)
    format_s_int(df, columns4s_int)
    format_numeric(df, columns4num)
    format_float(df, columns4float)
    format_str(df, main_param['column_sizes'], columns4str)

    df = df.replace({np.nan: None})

    return df


def main_process_xml(main_param):
    # root = ET.parse(main_param['xml_file']).getroot()
    # main_node = root.iterfind(main_param['xml_findkey'])

    part_dict = {}
    rec_count = main_param['max_records']
    part_count=0
    df_count=0

    add_empty_row2dict(part_dict, main_param['table_columns'])

    for event, elem in ET.iterparse(main_param['xml_file']):
        if elem.tag == main_param['xml_findkey'] and part_dict:
            rec_count -= 1
            if rec_count == 0:
                # Формирование и подготовка df для записи базы данных
                df = prepare_df(part_dict, main_param)
                df_count+=len(df)
                #db_lib.insert2Table(main_param['connection'], df, main_param['table'], main_param['unique_key'])
                db_lib.insert2Table_batch(main_param['connection'], df, main_param['table'], main_param['unique_key'])
                part_count += 1
                print(f"Записали {df_count} по  {len(df)} записей--- {datetime.datetime.now()} ---")
                part_dict = {}
                rec_count = main_param['max_records']
            add_empty_row2dict(part_dict, main_param['table_columns'])
        else:
            change_elem2dict(elem, main_param['table_columns'], part_dict)
            #add_elem2dict(elem, main_param['table_columns'], part_dict)

    if part_dict:
        df = prepare_df(part_dict, main_param)
        df = df[:-1]
        df_count+=len(df)
        db_lib.insert2Table_batch(main_param['connection'], df, main_param['table'], main_param['unique_key'])
        part_count += 1
        print(f"Записали {df_count} по  {len(df)} записей--- {datetime.datetime.now()} ---")
