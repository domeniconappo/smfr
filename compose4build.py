import sys
import os


def do(image_tag):
    for folder in ('annotator', 'geocoder', 'persister', 'rest_server', 'web'):
        docker_in = os.path.join(folder, 'Dockerfile')
        docker_out = os.path.join(folder, 'Dockerfile.ready')
        with open(docker_out, 'w') as f, open(docker_in) as i:
            read = i.read()
            wrote = read.replace('${IMAGE_TAG}', image_tag)
            f.write(wrote)


if __name__ == '__main__':
    args = sys.argv[1:]
    tag = args[0]
    sys.exit(do(tag))
