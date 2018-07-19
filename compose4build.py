import sys


def do(image_tag):
    for component in ('annotator', 'geocoder', 'persister', 'restserver', 'web', 'aggregator',):
        docker_in = 'Dockerfile.{}'.format(component)
        docker_out = 'Dockerfile.{}.ready'.format(component)
        with open(docker_out, 'w') as f, open(docker_in) as i:
            read = i.read()
            wrote = read.replace('${IMAGE_TAG}', image_tag)
            f.write(wrote)


if __name__ == '__main__':
    args = sys.argv[1:]
    tag = args[0]
    sys.exit(do(tag))
