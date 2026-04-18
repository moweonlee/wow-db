구현에서는 다음과 같은 철학이 반드시 반영되어야 합니다.
- SIMD 를 활용할 수 있도록 Rust 를 사용하되 꼭 필요한 곳은 C++ 을 사용할 수 있습니다. 그러나 되도록이면 RUST 가 되어야 합니다.
- 각각의 모듈별로 별도의 TOp Directory 가 나뉘어져야 합니다. 
  - Query Node
  - Compute Node
  - Storage Node
- 각각의 모듈을 한번에 Local 에서 쉽게 테스틓할 수 있는 Docker Compose 파일이 필요합니다.
