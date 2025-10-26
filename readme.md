In order vscode not able to run code due to : 
Caused by: java.lang.RuntimeException: java.lang.reflect.InaccessibleObjectException: Unable to make field private final java.lang.Object[] java.util.Arrays$ArrayList.a accessible: module java.base does not "opens java.util" to unnamed module @4fcb38dd

configure the .vscode/launch.json
{
    "version": "0.2.0",
    "configurations": [
        {
            "type": "java",
            "name": "EnrichmentSimpleApp",
            "request": "launch",
            "mainClass": "flink.self.traning.statefulTransformation.EnrichmentSimpleApp",
            "projectName": "app",
            "vmArgs": "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
        },
        {
            "type": "java",
            "name": "Current File",
            "request": "launch",
            "mainClass": "${file}",
            "projectName": "app",
            "vmArgs": "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
        }
    ]
}
