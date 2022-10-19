#!~/anaconda3/bin/python

import os, sys, getopt, re

def parse_arguments(argv):
    # defaults
    target_folder = '.'
    overwrite = False
    try:
        opts, args = getopt.getopt(argv, "hi:r:o:ws")
    except:
        print("Error in parsing inputs.")
        print("Usage: check_archives.py -o <target_folder> [-w]")
    for opt, arg in opts:
        if opt == '-h':
            print("Usage: check_archives.py -o <target_folder> [-w]")
            print(" -w: overwrite existing files")
        elif opt in ['-o']:
            target_folder = arg
        elif opt in ['-w']:
            overwrite = True

    print(f"* Checking archives from extract_fov_files.py. ")
    print(f"-- by Pu Zheng, 2022.10.19")
    print(f"-- target_folder: {target_folder}")
    print(f"-- overwrite target files: {overwrite}")
    return target_folder, overwrite


if __name__ == "__main__":
    # parse
    target_folder, overwrite = parse_arguments(sys.argv[1:])
    # trim folder filepath
    if target_folder[-1]  == os.path.sep:
        target_folder = target_folder[:-1]
    # searching for file-lists and archive-scanning-logs
    _fovs, _filelist_files, _scanning_log_files = [], [], []
    print("searching for filelist and scanning-log, found fov: ")
    for _file in os.listdir(target_folder):
        _full_file = os.path.join(target_folder, _file)
        if os.path.isfile(_full_file):
            # check if its a filelist item
            re_string = r'filelist_(?P<fov>[0-9]+)\.txt$'
            _match_result = re.match(re_string, _file)
            if _match_result:
                _fov = _match_result.groupdict()['fov']
                _log_file = os.path.join(target_folder, f"Fov_{_fov}.log")
                # append if log_file also exists
                if os.path.exists(_log_file):
                    print(f"{_fov}", end=', ')
                    _fovs.append(_fov)
                    _filelist_files.append(_full_file)
                    _scanning_log_files.append(_log_file)
    print("")
    # running checks
    _fov_checks = {}
    for _fov, _filelist_file, _log_file in zip(_fovs, _filelist_files, _scanning_log_files):
        # filelist
        _filelist = open(_filelist_file, 'r').readlines()
        _filelist = [_l.rstrip() for _l in _filelist]
        # filename in archive log:
        _archived_filelist = open(_log_file, 'r').readlines()
        _archived_filelist = [_l.rstrip() for _l in _archived_filelist]
        # comapre
        _good_archive = True
        for _file in _filelist:
            if _file not in _archived_filelist:
                _good_archive = False
                break
        # append
        _fov_checks[_fov] = _good_archive
    # save results:
    checking_result_file = os.path.join(target_folder, 'checking_archive_result.csv')
    if not os.path.exists(checking_result_file) or overwrite:
        print(f"Checking result saved into file: {checking_result_file}")
        with open(checking_result_file, 'w', encoding='utf-8') as _sf:
            _sf.write('Fov,Good_archive\n')
            for _fov, _good_archive in _fov_checks.items():
                _sf.write(f'{_fov},{_good_archive}\n')
    # print results:
    _num_archives = len(_fov_checks)
    _num_good_archives = len([_v for _v in _fov_checks.values() if _v])
    print(f"* {_num_archives} field-of-view checked.")
    print(f"* {_num_good_archives} archives are good.")
    if _num_good_archives == _num_archives:
        print("* All set!")
    else:
        print("bad fovs are:", end=' ')
        for _fov, _good_archive in _fov_checks.items():
            if not _good_archive:
                print(f"{_fov}", end=', ')
        print("")



