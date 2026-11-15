import argparse
from . import prepare, explore, enrich, streaming

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("command",choices=["prepare","explore","enrich","stream"])
    cmd=parser.parse_args().command
    {"prepare":prepare.run,"explore":explore.run,"enrich":enrich.run,"stream":streaming.run}[cmd]()
if __name__=="__main__": main()
