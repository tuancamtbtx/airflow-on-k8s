from airfactory.core.yaml import YamlReader

def test_yaml_config():
    path = "./tests/dags/test.yaml"
    yaml_conf = YamlReader()
    content = yaml_conf.read(path)
    print(content)

test_yaml_config()