import configparser
import sys

from pathlib import Path

from tqdm import tqdm


def main():
    heads = {}
    for cf in tqdm(Path(sys.argv[1]).rglob("*.siva")):
        config = configparser.ConfigParser()
        config.read(cf)
        for section in config.sections():
            if section.startswith("remote"):
                url = config[section]["url"].strip()
                url = url.replace("git://github.com/", "")
                if url.endswith(".git"):
                    url = url[:-4]
                uuid = section[8:-1]
                if heads.get(uuid, url) != url:
                    raise KeyError("%s: %s vs %s" % (uuid, heads[uuid], url))
                heads[uuid] = url
    with open("heads.csv", "w") as fout:
        for k, v in heads.items():
            fout.write("%s,%s\n" % (k, v))


if __name__ == "__main__":
    sys.exit(main())
