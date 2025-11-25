# CPQ Native Index Integration Notes

- The build now consumes the fork located at `../CPQ-native-index/CPQ-native Index` via a Gradle composite (`includeBuild`). Gradle automatically rebuilds the fork whenever `dev.roanh.cpqnativeindex:cpq-native-index:1.0` is requested, so keep the fork checked out next to this repository.
- The fork has been updated to depend on gMark `2.1`, letting this project use the published gMark API directly. The historical compatibility shims under `dev/roanh/gmark/**` have been removed.
- Graph loading now relies directly on `dev.roanh.cpqindex.IndexUtil.readGraph`, since the native index dependency already provides the helper.

If the fork is moved, update `settings.gradle` accordingly so the composite build still resolves the dependency locally.
