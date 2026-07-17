수정 사항 Changelog

v 0.0.24
- 거실 1/2 조명 ON 패킷에서 잘못 사용하던 `F1` 상태 바이트를 표준 `01`로 복구
- 구 EzVille RS485 Addon과 동시 실행 시 소켓/엔티티 충돌하므로 Simple 애드온만 사용

v 0.0.23
- MQTT mode + EW11 TCP fallback 환경에서 조명/월패드 명령도 MQTT ew11/send가 아니라 실제 연결된 TCP socket으로 전송

v 0.0.22
- EW11 TCP fallback에서 짧은 plug 상태 패킷이 들어와도 수신 루프가 오류 폭주하지 않도록 방어

v 0.0.21
- EW11 TCP fallback 활성화 시 헬스체크도 socket 수신 타이머를 보도록 수정

v 0.0.20
- MQTT mode에서도 EW11 TCP 수신 fallback을 기본 활성화해 ew11/recv MQTT가 멈춰도 상태 패킷을 직접 수신

v 0.0.19
- EW11/RS485 수신 헬스체크를 일반 MQTT 트래픽과 분리
- HA status/discovery 메시지만 들어오는 상태에서 수신 루프가 죽어도 자동 복구/재시작되도록 수정
- 헬스체크 회귀 테스트 추가

v 0.0.1
준비중
