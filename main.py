"""
This is entry file for this project

Date: 2020/10/30
"""

if __name__ == '__main__':
    # This wired import position is for multiprocessing on Windows
    from Pybase.__main__ import main, get_parser
    main(get_parser().parse_args())
