import datetime
import re
from urllib import parse
import pathlib
import uuid
from typing import List, Dict, Any, Union
from pathlib import Path
from collections import namedtuple

from bs4 import BeautifulSoup
from requests_html import HTMLSession
import pandas as pd
import icalendar
from slugify import slugify


def get_cities() -> List[Dict[str, Any]]:
    soup = _get_page({})
    cities = []
    for city in soup.find_all('tr'):
        if city.text:
            cities.append(
                {'gem_nr': int(parse.parse_qs(city.a.attrs['href'][1:])['gem_nr'][0]),
                 'gemeinden': city.text.strip()}
            )
    return cities


def _get_page(payload):
    payload.update({'kat': 32, 'portal': 'verband', 'vb': 'bn'})
    with HTMLSession() as session:
        page = session.get("https://baden.umweltverbaende.at/", data=payload)
        page.html.render(timeout=20)
    soup = BeautifulSoup(page.content, features='lxml')
    return soup


def get_city_timetable(city: Dict[str, Any]) -> pd.DataFrame:
    timetable = _get_city_timetable_raw(city)
    timetable = _process_timetable(timetable)
    return timetable


def _get_city_timetable_raw(city: Dict[str, Any]) -> List[str]:
    soup = _get_page({'jahr': 2021, 'gem_nr': city['gem_nr']})
    return [x.text.strip() for x in soup.find_all(class_='tunterlegt')]


def _process_timetable(timetable: List[str]) -> pd.DataFrame:
    termine = [re.search(r"(\d{2}.\d{2}.\d{4})", date).group(1) for date in timetable]
    termine = pd.to_datetime(termine, format='%d.%m.%Y')
    sorte = [re.search(r"(Biotonne|Restmüll|Altpapier|Gelber Sack)", date).group(1) for date in timetable]
    abfuhrbereich = [re.search(r"Abfuhr(bereich|gebiet) (.+):", date).group(2)
                     if 'Abfuhr' in date else None for date in timetable]
    if not any(abfuhrbereich):
        abfuhrbereich = '1'
    return pd.DataFrame({'Termine': termine, 'Sorte': sorte, 'Abfuhrbereich': abfuhrbereich})


def _dates2cal(dataframe: pd.DataFrame, fname: str):
    cal = icalendar.Calendar()
    cal.add('prodid', '-//GVA Baden//')
    cal.add('version', '2.0')
    for date in dataframe.itertuples():
        cal.add_component(_date2event(date))
    with open(fname, 'wb') as file_handle:
        file_handle.write(cal.to_ical())


def _date2event(date: namedtuple) -> icalendar.Event:
    event = icalendar.Event()
    event['SUMMARY'] = date.Sorte
    event['dtstart'] = icalendar.vDate(date.Termine)
    event['dtstamp'] = icalendar.vDatetime(datetime.datetime.now())
    event['uid'] = uuid.uuid1().hex
    return event


def city2cal(city: Dict[str, Any], output_dir: Union[str, Path] = '') -> None:
    for abfuhrbereich in city['termine'].Abfuhrbereich.unique():
        if abfuhrbereich:
            file_name = f"{output_dir}/{'_'.join([slugify(x) for x in [city['gemeinden'], abfuhrbereich]])}.ics"
            dataframe = city['termine'].query(f'Abfuhrbereich in ["{abfuhrbereich}", None]')
            _dates2cal(dataframe, file_name)


def main(project_dir: Union[str, Path] = '') -> None:
    output_dir = pathlib.Path(f"{project_dir}/calendars")
    output_dir.mkdir(exist_ok=True)
    cities = get_cities()
    for city in cities:
        print(city['gemeinden'])
        city['termine'] = get_city_timetable(city)
        city2cal(city, output_dir)


if __name__ == '__main__':
    import git
    main(git.Repo('.', search_parent_directories=True).working_tree_dir)
