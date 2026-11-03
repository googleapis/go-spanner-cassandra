# Changelog

## [0.7.0](https://github.com/googleapis/go-spanner-cassandra/compare/v0.6.1...v0.7.0) (2026-06-03)


### Features

* add support for USE keyspace attachments ([e54c1dc](https://github.com/googleapis/go-spanner-cassandra/commit/e54c1dcd17cd67c0928d6a5f70810ac0fa789cbb))
* support USE keyspace attachments ([ff4d0d0](https://github.com/googleapis/go-spanner-cassandra/commit/ff4d0d047101446d4af7f5213fb92a313debdc88))

## [0.6.1](https://github.com/googleapis/go-spanner-cassandra/compare/v0.6.0...v0.6.1) (2026-05-19)


### Bug Fixes

* errors ([a918dd5](https://github.com/googleapis/go-spanner-cassandra/commit/a918dd57ce3b94f0515a70acb7ac08d6f7df7706))
* update TestBatchLimit to reflect new Spanner limits ([1d79ff2](https://github.com/googleapis/go-spanner-cassandra/commit/1d79ff2691af31f7bfbec4148ac816fa25119bbd))
* update TestBatchLimit to reflect new Spanner limits ([58400d6](https://github.com/googleapis/go-spanner-cassandra/commit/58400d6111a50224b66cad6c99147e34d47626d3))

## [0.6.0](https://github.com/googleapis/go-spanner-cassandra/compare/v0.5.0...v0.6.0) (2026-05-04)


### Features

* add TLS support to TCP proxy ([5bc047e](https://github.com/googleapis/go-spanner-cassandra/commit/5bc047e2465c8cc71e764dfd681d4606a96e0b87))
* add TLS support to TCP proxy ([f17dde7](https://github.com/googleapis/go-spanner-cassandra/commit/f17dde79d0592097b8740157ce1f1934bfe892f6))
* expose options for connecting to experimental host via tls/mTLS ([07ea31a](https://github.com/googleapis/go-spanner-cassandra/commit/07ea31ace8421cb225d4557cb6ea673498c53b95))
* expose options for connecting to experimental host via tls/mTLS ([fa15499](https://github.com/googleapis/go-spanner-cassandra/commit/fa15499274fc0f6220c3eec44879cd4c1e94b338))
* expose options to override spanner endpoint and talking to spanner insecurely ([#67](https://github.com/googleapis/go-spanner-cassandra/issues/67)) ([b85bd6f](https://github.com/googleapis/go-spanner-cassandra/commit/b85bd6f4951ce29d63bf1bee0c9dff59c9f5f746))

## [0.5.0](https://github.com/googleapis/go-spanner-cassandra/compare/v0.4.0...v0.5.0) (2025-07-23)


### Features

* support setting global max commit delay for all dmls ([761e8c8](https://github.com/googleapis/go-spanner-cassandra/commit/761e8c8852ed3e0c30388db517a6c1608a1cf4bd))


### Bug Fixes

* clear header flags in manually constructed error response ([3327059](https://github.com/googleapis/go-spanner-cassandra/commit/332705969663e021c4df71bd0574e6293ae0edc0))


### Documentation

* add unsupported features from apache/gocql ([509cb47](https://github.com/googleapis/go-spanner-cassandra/commit/509cb474017dcc513894935727baab8004652c5b))
* update supported versions in README ([564c147](https://github.com/googleapis/go-spanner-cassandra/commit/564c1476213710dac3deb528de97b6113aeeb942))

## [0.4.0](https://github.com/googleapis/go-spanner-cassandra/compare/v0.3.0...v0.4.0) (2025-05-08)


### Features

* enable leader aware routing by default for all write operations ([4678fb5](https://github.com/googleapis/go-spanner-cassandra/commit/4678fb56ed784a3346f106d30a4947a81c2da912))

## [0.3.0](https://github.com/googleapis/go-spanner-cassandra/compare/v0.2.0...v0.3.0) (2025-04-23)


### Features

* support suppress api client options on start ([7bdfd24](https://github.com/googleapis/go-spanner-cassandra/commit/7bdfd2402806320e9f43d1dc8818c527f00d2d36))


### Documentation

* add log level config to readme ([58fe77c](https://github.com/googleapis/go-spanner-cassandra/commit/58fe77c89ce512454c0e3134a04ca648c8e7e33b))

## [0.2.0](https://github.com/googleapis/go-spanner-cassandra/compare/v0.1.0...v0.2.0) (2025-04-16)


### Features

* support configurable log level on the global zap logger ([880b547](https://github.com/googleapis/go-spanner-cassandra/commit/880b5475b82493db388b6cefd5ab5d198d44e65f))


### Documentation

* add dynamic fetching of current release version and go version in readme ([07b7316](https://github.com/googleapis/go-spanner-cassandra/commit/07b7316c505402ef724ace47385ab7313c4737c0))
* allow external contributions ([4f39b18](https://github.com/googleapis/go-spanner-cassandra/commit/4f39b1868cef25aa5bc48f220f524210cafa9559))
* automatically update versions in readme and user agent str ([9a0675f](https://github.com/googleapis/go-spanner-cassandra/commit/9a0675f0e8e127c8ab0ea1c9daa79a40cd69613d))
* fix sample lint ([187deb6](https://github.com/googleapis/go-spanner-cassandra/commit/187deb6a2e9be77928df5b94489999300c26b4f7))

## 0.1.0 (2025-04-09)


### Features

* Initial commit to add go-spanner-cassandra ([760c898](https://github.com/googleapis/go-spanner-cassandra/commit/760c89882f33d7b2395c06c1f53ac6620de133f6))


### Documentation

* Add issue templates and codeowners ([ad762fb](https://github.com/googleapis/go-spanner-cassandra/commit/ad762fbe13e05848cc9358759a3b3ec39764d930))
* Fix readme lint and add redirect to schema converter tool ([0990112](https://github.com/googleapis/go-spanner-cassandra/commit/09901122bdfdba9e522fa4a844d6a5cba131760e))
* Update contributing guide ([a240041](https://github.com/googleapis/go-spanner-cassandra/commit/a2400411d5e2c7519b08e21e4d8101b458bbaccd))
* Use consistent naming in readme ([4ab78d0](https://github.com/googleapis/go-spanner-cassandra/commit/4ab78d0c1db65505837624ada6bc90ea9c1a9b74))
