import sys
import os


def read_env():
    split_properties = [line.split("=") for line in open('.env') if line and line != '\n' and not line.startswith('#')]
    props = {key: value.strip() for key, value in split_properties}
    return props


properties = read_env()


def replace_image_variable(component_folder, variable):
    dockerfile_in = os.path.join(component_folder, 'Dockerfiletemp')
    dockerfile_out = os.path.join(component_folder, 'Dockerfile')
    with open(dockerfile_in, 'r') as fin, open(dockerfile_out, 'w') as fout:
        read = fin.read()
        wrote = read.replace('${%s}' % variable, properties[variable])
        fout.write(wrote)


def do(image_tag):
    replace_image_variable('./base', 'PYTHON_BASE_IMAGE')
    replace_image_variable('./cassandrasmfr', 'CASSANDRA_BASE_IMAGE')
    replace_image_variable('./mysql', 'MYSQL_BASE_IMAGE')
    replace_image_variable('./geonames', 'GEONAMES_BASE_IMAGE')
    replace_image_variable('./backupper', 'ALPINE_IMAGE')

    for component in ('annotator', 'geocoder', 'persister', 'restserver', 'web', 'aggregator', 'products',):
        docker_in = 'Dockerfile.{}'.format(component)
        docker_out = 'Dockerfile.{}.ready'.format(component)
        with open(docker_out, 'w') as fout, open(docker_in) as fin:
            read = fin.read()
            if component != 'web':
                wrote = read.replace('${SMFR_IMAGE}', properties['SMFR_IMAGE'])
            else:
                wrote = read.replace('${WEB_BASE_IMAGE}', properties['WEB_BASE_IMAGE'])
            wrote = wrote.replace('${IMAGE_TAG}', image_tag)
            fout.write(wrote)


if __name__ == '__main__':
    args = sys.argv[1:]
    tag = args[0]
    sys.exit(do(tag))
