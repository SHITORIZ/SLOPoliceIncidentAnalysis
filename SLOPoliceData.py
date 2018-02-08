import pandas as pd
import numpy as np
import glob

files = glob.glob('/data/slo_police_logs_2017-02/*2016-08*.txt')

MARKER = '====='


def parse_header(text):
    H ={}
    text = text.split(" ")
    text = filter(bool, text)
    text = list(text)
    text =  [line.lower() for line in text]
    for line in text:
        if line[0:8] == 'received':
            H[line[0:8]] = line[9:]
        elif line[0:7] == 'arrived':
            H[line[0:7]] = line[8:]
        elif line[0:7] == 'cleared':
            H[line[0:7]] = line[8:]
        elif line[0:10] == 'dispatched':
            H[line[0:10]] = line[11:]
        elif line[2] == '/':
            H['date'] = line[0:]
        else:
            H['id'] = line[0:]
            
    return H


def parse_type_location(text):
    """Parse the type/location line of text into a dict."""
    loc_and_type = {}
    type_index = text.find('Type')
    loc_index = text.find('Location')
    
    #extract type andn location in string
    t_ype = text[type_index:loc_index].lower()
    location = text[loc_index:].lower()
    
    #split type, get rid of empty strings, turn back into a list
    t_ype = t_ype.split(':')
    t_ype[1] = t_ype[1].strip(' ')
    
    #split location to separate key from value
    location = location.split(':')
    #put in dictionary
    loc_and_type[t_ype[0]] = t_ype[1]
    loc_and_type[location[0]] = location[1]
    
    
    return loc_and_type


def parse_units(text):
    """Parse the line containing `Units:` into a dict."""
    unit_dict = {}
    text = text.lower()
    text = text.split(":")
    if text[1]:
        text[1] = text[1].split(',')
        i = 0
        while i < len(text[1]):
            text[1][i] = text[1][i].strip(' ')
            i+=1
    elif not text[1]:
        text[1] = list(text[1])
        
    unit_dict[text[0]] = text[1]
    return unit_dict


def parse_incident(text):
    """Parse an indicent entry into a python dict."""
    text = text.splitlines()
    
    type_line = ""
    unit_line = ""
    i = 0;
    while i < len(text):
        if text[i][0:5] == 'Type:':
            type_line = text[i]
        elif text[i][0:6] == 'Units:':
            unit_line = text[i]
            break
            
        i+=1
    
    parsed_dict ={}
    Header = text[1]
    Type = type_line
    Units = unit_line
    H_dict = parse_header(Header)
    T_dict = parse_type_location(Type)
    U_dict = parse_units(Units)
    parsed_dict.update(H_dict)
    parsed_dict.update(T_dict)
    parsed_dict.update(U_dict)
    return parsed_dict



def files_to_lines(files):
    """Take a list of files and yield their lines."""
    for filename in files:
        with open(filename) as file:
            data = file.read()
            data = data.splitlines()
            for lines in data:
                yield lines.strip()


def lines_to_indicents(lines):
    """Consume an iterator of lines, yield text blocks of for each incident."""
    next(lines)
    next(lines)
    next(lines)
    next(lines)
    next(lines)
    
    new_list = []
    marker_tracker = 0
    
    for line in lines:
        if MARKER in line:
            marker_tracker +=1
        if marker_tracker == 3:
            yield "\n".join(new_list)
            marker_tracker = 1
            new_list = []
            
        new_list.append(line)


#using generators to get on entry at a time and storing the dictionaries in
#a incident list
lines = files_to_lines(files)
police_incidents = lines_to_indicents(lines)
incidents = []
data = {}

for entry in police_incidents:
    data = parse_incident(entry)
    incidents.append(data)

#Create a dataframe of the incidents
incidents_df = pd.DataFrame(incidents, columns = ['arrived', 'cleared', 'date', 'dispatched', 'id', 'location', 'received', 'type'])
incidents_df.head()


#Created a second DataFrame, named units_df with two columns: The id of the incident and unit that handled the call
The unit that handled the call
id_units = []
for incident in incidents:
    units = incident['units']
    for unit in units:
        id_units.append({'id':incident['id'], 'unit': unit})

units_df = pd.DataFrame(id_units, columns = ['id', 'unit'])
units_df['unit'].replace('', np.nan, inplace=True)
units_df.dropna(subset=['unit'], inplace=True)

#created a visualization of the original incidents_df data 
#which conclude that the locations with largers circles had more of the associated crime
import altair as alt
alt.enable_mime_rendering()

alt.Chart(incidents_df).mark_point().encode(
    alt.X('location:N'),
    alt.Y('type'),
    alt.Size('count(*)')

)





