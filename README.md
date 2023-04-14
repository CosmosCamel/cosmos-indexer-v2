# cosmos-indexer-v2

This harvester downloads blocks from an RPC node, multithreaded over an input range of block heights.

It's optimized to bring the maximum performance per one CPU core. Can run more than a single instance to further increase performance over multiple cores.

To-Do: 
- [ ] Process output files to create bulked input for lightweight junod decoder
- [ ] Optimize mysql data handling and storage efficiency
- [ ] Include protobuf decoding in logic and exclude calling junod (insignificant perf improvement, not a priority)
- [ ] Test and serve
