import pandas as pd
import camelot
import json
from datetime import datetime

from write_to_gbq import data_writer

folder = "/home/pauloandrade/Documents/COVID-19 DATA/WHO Reports/"
table_name='data_by_country_'

file = [
        ["20200504-covid-19-sitrep-105.pdf",[6,7,8,9,10,11,12,13,14,15]]]
date_array = ["2020-05-04"]

changes_data=pd.DataFrame(
                columns=['date', 'changes']
            )

credentials = json.load(open(
    "/home/pauloandrade/Documents/Credentials/"+
    "Pessoal - Google/analog-delight-253221-4e61977765fe.json", "r"
))

covid_data_schema=[
    {'name':'date', 'type':'DATE'},
    {'name':'country_name', 'type':'STRING'},
    {'name':'total_confirmed_infected', 'type':'INTEGER'},
    {'name':'total_deaths', 'type':'INTEGER'}
]

changes_data_schema=[
    {'name':'date', 'type':'STRING'},
    {'name':'changes', 'type':'STRING'}
]

default_columns_upto_april=[
    'country_name',
    'total_confirmed_infected',
    'total_confirmed_new_cases',
    'total_deaths',
    'total_new_deaths',
    'transmission_classification',
    'days_since_last_reported_case'
]

default_columns=[
    'country_name',
    'total_confirmed_infected',
    'total_confirmed_new_cases',
    'total_deaths',
    'total_new_deaths'
]

drop_columns_upto_april=[
    'total_confirmed_new_cases',
    'total_new_deaths',
    'transmission_classification',
    'days_since_last_reported_case'
]

drop_columns=[
    'total_confirmed_new_cases',
    'total_new_deaths'
]

from_to={
    "wrong_values":["Kosovo[1]","Northern Mariana","occupied Palestinian territory",
            "Bolivia (Plurinational State of)","Venezuela (Bolivarian Republic of)",
            "Democratic Republic", "Saint Barthelemy", "the)", "wealth of the)",
            "Northern Mariana Islands (Commonwealth of the)"
    ],
    "correct_values":["Kosovo","Northern Mariana Islands","Palestine",
            "Bolivia","Venezuela",
            "Lao People's Democratic Republic", "Saint Barthélemy",
            "Northern Mariana Islands", "Northern Mariana Islands",
            "Northern Mariana Islands"
    ]
}

all_countries = []
countries_file = open("all_countries_available", "r")
for line in countries_file:
    line = line.rstrip()
    all_countries.append(line)

def clean_data_upto_april(raw_data, date):
        if raw_data[0][0]=='Territory/Area†':
            raw_data.drop(raw_data.head(1).index,inplace=True)
        print(raw_data)
        rename_dict = create_dict(list(raw_data.columns),default_columns_upto_april)

        raw_data = rename_columns(raw_data, rename_dict)

        raw_data = raw_data[raw_data['total_confirmed_infected']!=''].copy()
        raw_data = raw_data[raw_data['country_name']!=''].copy()
        correct_country_name(raw_data, from_to)

        raw_data.drop(raw_data.tail(3).index,inplace=True)
        raw_data.drop(columns=drop_columns_upto_april,inplace=True)
        raw_data.insert(0, "date", date)
        return raw_data

def convert_pdf_to_csv_upto_april(folder, file):
    table = pd.DataFrame()
    print("Reading data from file "+file[0])
    for j in file[1]:
        print("Page "+ str(j))
        if table.empty:
            table = camelot.read_pdf(
                folder+file[0],
                pages = str(j),
                flavor='stream',
                edge_tol=120,
                table_areas=['0,700,566,50'],
                row_tol=13,
                strip_text='\n'
            )[0].df.copy()
        else:
            table = table.append(camelot.read_pdf(
                folder+file[0],
                pages = str(j),
                flavor='lattice',
                line_scale=50,
                shift_text=['l'],
                strip_text='\n')[0].df.copy()
            )
    table.reset_index(inplace=True, drop=True)
    return table

def clean_data(raw_data, date):
        rename_dict = create_dict(list(raw_data.columns),default_columns)

        raw_data = rename_columns(raw_data, rename_dict)
        raw_data.drop(raw_data.head(1).index,inplace=True)
        raw_data = raw_data[raw_data['total_confirmed_infected']!=''].copy()
        raw_data = raw_data[raw_data['country_name']!=''].copy()
        correct_country_name(raw_data, from_to)

        raw_data.drop(raw_data.tail(3).index,inplace=True)
        raw_data.drop(columns=drop_columns,inplace=True)
        raw_data.insert(0, "date", date)
        return raw_data

def convert_pdf_to_csv(folder, file):
    table = pd.DataFrame()
    print("Reading data from file "+file[0])
    for j in file[1]:
        print("Page "+ str(j))
        table = table.append(camelot.read_pdf(
                folder+file[0],
                pages = str(j),
                flavor='stream',
                edge_tol=70,
                table_areas=['0,505,550,30'],
                columns=['200,300,400,500'],
                row_tol=13,
                strip_text='\n'
            )[0].df.copy()
        )
    table.reset_index(inplace=True, drop=True)
    return table

def create_dict(list1, list2):
    result_dict={}
    for elem in range(len(list1)):
        result_dict[list1[elem]] = list2[elem]
    return result_dict

def rename_columns(table, column_dict):
    return table.rename(columns=column_dict, copy=True)

def correct_country_name(table, value_dict):
    for value in range(len(value_dict['wrong_values'])):
        table['country_name'].where(
            table['country_name']!=value_dict['wrong_values'][value],
            value_dict['correct_values'][value],
            inplace=True
        )

def main():
    i=0
    while i < len(file):
        raw_data = convert_pdf_to_csv(folder, file[i][:])
        raw_data = clean_data(raw_data, date_array[i])
        print(raw_data)
        raw_data.to_csv("/home/pauloandrade/Documents/COVID-19 DATA/"+
            table_name+str(date_array[i]).replace("-","_")+".csv", index=False
        )

        for country_name in all_countries:
            if country_name not in list(raw_data['country_name']):
                print("Warning: missin data for "+country_name)
        if len(raw_data) !=214:
            print("Warning: Please check if output is correct (" + str(len(raw_data)) + " rows)")

        print("Please check data output")
        user_choice = input("Upload data to Google Big Query? (Y/n) ")

        if user_choice=='Y':
            raw_data = pd.read_csv("/home/pauloandrade/Documents/COVID-19 DATA/"+
                table_name+str(date_array[i]).replace("-","_")+".csv", index_col=False
            )
            print("Uploading data to Google Big Query")
            data_writer(
                raw_data,
                "analog-delight-253221",
                "COVID_19",
                table_name+str(date_array[i]).replace("-","_"),
                credentials,
                "replace",
                covid_data_schema
            )

            changes_data.loc[0]=(
                [datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"),
                    str(date_array[i])+' data commit.']
            )

            data_writer(
                changes_data,
                "analog-delight-253221",
                "COVID_19",
                "version_control",
                credentials,
                "append",
                changes_data_schema
            )
        else:
            pass
        i+=1

if __name__ == "__main__":
    main()