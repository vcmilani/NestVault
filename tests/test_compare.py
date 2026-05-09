from conftest import make_backup, make_version, upload_file, finish_version


def setup_two_versions(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    make_version(client, "b1", "v2")
    return "b1", "v1", "v2"


def test_compare_added(client):
    label, v1, v2 = setup_two_versions(client)
    upload_file(client, label, v1, path="/old.txt", content=b"old")
    upload_file(client, label, v2, path="/old.txt", content=b"old")
    upload_file(client, label, v2, path="/new.txt", content=b"new")

    r = client.get(f"/backups/{label}/compare", params={"v1": v1, "v2": v2})
    assert r.status_code == 200
    data = r.json()
    assert len(data["added"]) == 1
    assert data["added"][0]["original_path"] == "/new.txt"


def test_compare_deleted(client):
    label, v1, v2 = setup_two_versions(client)
    upload_file(client, label, v1, path="/removed.txt", content=b"bye")
    upload_file(client, label, v1, path="/kept.txt", content=b"kept")
    upload_file(client, label, v2, path="/kept.txt", content=b"kept")

    r = client.get(f"/backups/{label}/compare", params={"v1": v1, "v2": v2})
    data = r.json()
    assert len(data["deleted"]) == 1
    assert data["deleted"][0]["original_path"] == "/removed.txt"


def test_compare_modified(client):
    label, v1, v2 = setup_two_versions(client)
    upload_file(client, label, v1, path="/f.txt", content=b"v1 content")
    upload_file(client, label, v2, path="/f.txt", content=b"v2 content changed")

    r = client.get(f"/backups/{label}/compare", params={"v1": v1, "v2": v2})
    data = r.json()
    assert len(data["modified"]) == 1
    mod = data["modified"][0]
    assert mod["original_path"] == "/f.txt"
    assert mod["v1_sha256"] != mod["v2_sha256"]


def test_compare_unchanged(client):
    label, v1, v2 = setup_two_versions(client)
    upload_file(client, label, v1, path="/same.txt", content=b"same")
    upload_file(client, label, v2, path="/same.txt", content=b"same")

    r = client.get(f"/backups/{label}/compare", params={"v1": v1, "v2": v2})
    data = r.json()
    assert data["summary_unchanged"] == 1
    assert data["added"] == []
    assert data["deleted"] == []
    assert data["modified"] == []


def test_compare_mixed(client):
    label, v1, v2 = setup_two_versions(client)
    upload_file(client, label, v1, path="/kept.txt",    content=b"same")
    upload_file(client, label, v1, path="/changed.txt", content=b"old")
    upload_file(client, label, v1, path="/gone.txt",    content=b"deleted")
    upload_file(client, label, v2, path="/kept.txt",    content=b"same")
    upload_file(client, label, v2, path="/changed.txt", content=b"new")
    upload_file(client, label, v2, path="/fresh.txt",   content=b"added")

    r = client.get(f"/backups/{label}/compare", params={"v1": v1, "v2": v2})
    data = r.json()
    assert data["summary_unchanged"] == 1
    assert len(data["added"]) == 1
    assert len(data["deleted"]) == 1
    assert len(data["modified"]) == 1


def test_compare_version_not_found(client):
    make_backup(client, "b1")
    make_version(client, "b1", "v1")
    r = client.get("/backups/b1/compare", params={"v1": "v1", "v2": "nope"})
    assert r.status_code == 404


def test_compare_size_delta(client):
    label, v1, v2 = setup_two_versions(client)
    upload_file(client, label, v1, path="/f.txt", content=b"short")
    upload_file(client, label, v2, path="/f.txt", content=b"longer content here")

    r = client.get(f"/backups/{label}/compare", params={"v1": v1, "v2": v2})
    mod = r.json()["modified"][0]
    assert mod["size_delta"] == len(b"longer content here") - len(b"short")
