"""TODO: DOCSTRING"""
import logging
import warnings
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from os import environ as osenviron
from pathlib import Path
from threading import Thread
from time import perf_counter
from typing import Annotated, Generator, Literal, Optional

import sqlalchemy
from pandas import DataFrame as Df
from pandas import concat as pd_concat
from pandas import read_excel as pd_read_excel
from sqlalchemy.dialects import postgresql
from yaml import Loader as YmlLoader
from yaml import load as ymlload

PERF_START = perf_counter()

APP_PATH = Path(osenviron['APP_PATH'])
DATA_PATH = Path(osenviron['DATA_PATH'])
CONN_STRING = osenviron['CONN_STRING']

_DBEngine = Annotated[sqlalchemy.engine.base.Engine,
                      "sqlalchemy.engine.base.Engine"]

LOGGER_FMT_TIME = '%x %X %Z'
LOGGER_FMT = \
    '[%(levelname)s] ' \
    + '%(asctime)s::%(name)s>>%(process)d:%(funcName)s: %(message)s'
logger = logging.getLogger('script-worker')
hdlr = logging.StreamHandler()
hdlr.setFormatter(
    fmt=logging.Formatter(fmt=LOGGER_FMT, datefmt=LOGGER_FMT_TIME))
logger.addHandler(hdlr=hdlr)
logger.setLevel(logging.INFO)

def _perf_parse(func_time) -> str:
    perf = perf_counter()
    return \
        f"Elapsed: {perf - func_time:.3f} (it) / {perf - PERF_START:.3f} (tot)"


def _detail_extract_each(path_: Path, cfg_: dict) -> Df:
    func_time = perf_counter()
    logger.info('Reading - %s', path_.name,)
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    df_extr: Df = pd_read_excel(
        io=path_,
        sheet_name=cfg_['sheet_name'],
        skiprows=cfg_['skiphead'],
        skipfooter=cfg_['skipfoot'],
        usecols=cfg_['usecols'],
    )
    logger.info('Successfully Read - %s | %s', path_.name,
                _perf_parse(func_time=func_time),)
    return df_extr


def _extract_detail(cfg_: dict, data_path: Path = DATA_PATH) \
        -> Generator[Df, None, None]:

    xl_list = (data_path / cfg_['src_dir']) \
        .glob(cfg_['glob'])

    futures: list[Future]
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(_detail_extract_each, _path, cfg_)
                   for _path in xl_list]
    for _future in as_completed(futures):
        yield _future.result()


def _extract_xlsx(io_path: Path, sheet_name_: str, usecols_: list[str]) -> Df:
    return pd_read_excel(io=io_path,
                         sheet_name=sheet_name_,
                         usecols=usecols_,)


def _extract_category_key(cfg_: dict, data_path: Path = DATA_PATH) -> Df:
    func_time = perf_counter()
    path_ = data_path / cfg_['src_file']
    df_ = _extract_xlsx(io_path=path_, sheet_name_=cfg_['sheet_name'],
                        usecols_=cfg_['usecols'],)

    logger.info('Successfully Read - %s | %s', path_.name,
                _perf_parse(func_time=func_time),)
    return df_

def _extract_outlet_key(cfg_: dict, data_path: Path = DATA_PATH
                        ) -> tuple[Df, dict]:
    func_time = perf_counter()
    path_ = data_path / cfg_['src_file']
    logger.info('Reading - %s', path_.name,)
    df_ = _extract_xlsx(
        io_path=path_,
        sheet_name_=cfg_['sheet_name'],
        usecols_=cfg_['usecols'],
    ) \
        .astype(cfg_['astype'])

    # Get sqlalchemy dtype for each field with the string in the config dict.
    #   Then add the sqlalchemy enum dtype object with the collected categories.
    type_enum = postgresql.ENUM(
        *list(df_[cfg_['type_col']].cat.categories),
        name=cfg_['enum_name']
    )
    dtype_ = \
        {
            k: getattr(postgresql, str(v))
            for k, v in cfg_['dtype'].items()
            if v
        } \
        | \
        {str(cfg_['type_col']): type_enum}

    logger.info('Successfully Read - %s | %s', path_.name,
                _perf_parse(func_time=func_time),)
    return df_, dtype_


def _load_to_db(df_: Df, table_name: str, db_: _DBEngine,
                if_exists_: Literal['fail', 'replace', 'append'] = 'replace',
                dtype: Optional[dict] = None, pre_sql: Optional[str] = None
                ):
    logger.info('Loading - %s', table_name,)
    func_time = perf_counter()
    if pre_sql:
        db_.execute(sqlalchemy.text(pre_sql))

    with db_.connect() as conn:
        df_.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists_,
            dtype=dtype,
        )
    logger.info('Successfully Loaded - %s | %s', table_name,
                _perf_parse(func_time=func_time),)


def _main():
    # Load arguments from yml file.
    cfgs = ymlload(
        (APP_PATH / 'cfg.yml').read_text(),
        YmlLoader
    )
    # Extract config arguments for each table.
    detail_cfgs, category_cfgs, outlet_cfgs = \
        (cfgs[label] for label in ('detail', 'category', 'outlet'))

    # Extra each table from source files.
    details_df = pd_concat(_extract_detail(detail_cfgs)) \
                 .rename(columns=detail_cfgs['rename'])
    outlet_df, outlet_dtype = _extract_outlet_key(outlet_cfgs)
    category_df = _extract_category_key(category_cfgs)

    # Load each to DB.
    db_eng = sqlalchemy.create_engine(url=CONN_STRING)

    load_jobs: list[Thread] = []
    for _df, _cfg, _dtype in (
        (details_df, detail_cfgs, None),
        (outlet_df, outlet_cfgs, outlet_dtype),
        (category_df, category_cfgs, None),
    ):
        job = Thread(target=_load_to_db,
                     kwargs={'df_': _df, 'table_name': _cfg['table_name'],
                             'db_': db_eng, 'dtype': _dtype,
                             'pre_sql': _cfg['pre_sql'],})
        job.start()
        load_jobs.append(job)
    for job in load_jobs:
        if job.is_alive():
            job.join()


if __name__ == "__main__":
    _main()
