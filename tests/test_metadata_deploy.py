import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib import metadata_deploy  # noqa: E402


def test_parse_soap_result_strips_namespaces_and_repeats():
    xml = """<?xml version="1.0"?>
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
      xmlns:met="http://soap.sforce.com/2006/04/metadata">
      <soapenv:Body>
        <met:checkDeployStatusResponse>
          <met:result>
            <met:id>0Afxx0000000001</met:id>
            <met:status>Failed</met:status>
            <met:done>true</met:done>
            <met:details>
              <met:componentFailures>
                <met:fileName>classes/Foo.cls</met:fileName>
                <met:problem>Unexpected token</met:problem>
              </met:componentFailures>
              <met:componentFailures>
                <met:fileName>classes/Bar.cls</met:fileName>
                <met:problem>Invalid type</met:problem>
              </met:componentFailures>
            </met:details>
          </met:result>
        </met:checkDeployStatusResponse>
      </soapenv:Body>
    </soapenv:Envelope>
    """

    result = metadata_deploy.parse_soap_result(xml, "result")

    assert result["id"] == "0Afxx0000000001"
    assert result["status"] == "Failed"
    failures = result["details"]["componentFailures"]
    assert isinstance(failures, list)
    assert failures[1]["fileName"] == "classes/Bar.cls"


def test_deploy_options_preserve_booleans_and_run_tests():
    xml = metadata_deploy.deploy_options_xml(
        {
            "check_only": True,
            "rollback_on_error": False,
            "single_package": True,
            "test_level": "RunSpecifiedTests",
            "run_tests": ["FooTest", "BarTest"],
        }
    )

    assert "<met:checkOnly>true</met:checkOnly>" in xml
    assert "<met:rollbackOnError>false</met:rollbackOnError>" in xml
    assert "<met:testLevel>RunSpecifiedTests</met:testLevel>" in xml
    assert xml.count("<met:runTests>") == 2


def test_deploy_options_parse_string_booleans():
    xml = metadata_deploy.deploy_options_xml(
        {
            "check_only": "false",
            "rollback_on_error": "true",
            "single_package": "false",
        }
    )

    assert "<met:checkOnly>false</met:checkOnly>" in xml
    assert "<met:rollbackOnError>true</met:rollbackOnError>" in xml
    assert "<met:singlePackage>false</met:singlePackage>" in xml


def test_summarize_result_extracts_counts_and_errors():
    details = {
        "status": "SucceededPartial",
        "done": "true",
        "success": "false",
        "numberComponentsDeployed": "3",
        "numberComponentsTotal": "4",
        "numberComponentErrors": "1",
        "numberTestsCompleted": "7",
        "numberTestsTotal": "8",
        "numberTestErrors": "1",
        "details": {
            "componentFailures": {"fileName": "classes/Foo.cls", "problem": "bad"},
            "runTestResult": {
                "failures": {"name": "FooTest", "message": "assertion failed"}
            },
        },
    }

    result = metadata_deploy.summarize_result(
        details,
        deploy_id="0Afxx0000000001",
        success_on_partial=True,
    )

    assert result["ok"] is True
    assert result["success"] is True
    assert result["counts"]["numberComponentsTotal"] == 4
    assert result["component_error_count"] == 1
    assert result["test_error_count"] == 1
    assert result["component_errors"][0]["fileName"] == "classes/Foo.cls"


def test_meaningful_status_changes_deduplicates_same_signature():
    statuses = [
        {"status": "Pending", "numberComponentsDeployed": "0"},
        {"status": "Pending", "numberComponentsDeployed": "0"},
        {"status": "InProgress", "numberComponentsDeployed": "1"},
        {"status": "InProgress", "numberComponentsDeployed": "2"},
    ]

    changes = list(metadata_deploy.meaningful_status_changes(statuses))

    assert [change[1]["status"] for change in changes] == [
        "Pending",
        "InProgress",
        "InProgress",
    ]
    assert changes[-1][1]["counts"]["numberComponentsDeployed"] == 2
