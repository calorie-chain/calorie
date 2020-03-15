package util



var (
	listenAddr            = "127.0.0.1:8805" 
	unSyncMaxTimes uint32 = 6                
	checkInterval  uint32 = 5                
)

type HealthCheckServer struct {
	api  client.QueueProtocolAPI
	l    net.Listener
	quit chan struct{}
	wg   sync.WaitGroup
}

func (s *HealthCheckServer) Close() {
	close(s.quit)
	s.wg.Wait()
	log.Info("healthCheck quit")
}

func NewHealthCheckServer(c queue.Client) *HealthCheckServer {
	if c == nil {
		return nil
	}
	h := &HealthCheckServer{}
	var err error
	h.api, err = client.New(c, nil)
	if err != nil {
		return nil
	}
	h.quit = make(chan struct{})
	return h
}

func (s *HealthCheckServer) Start(cfg *types.HealthCheck) {
	if cfg != nil {
		if cfg.ListenAddr != "" {
			listenAddr = cfg.ListenAddr
		}
		if cfg.CheckInterval != 0 {
			checkInterval = cfg.CheckInterval
		}
		if cfg.UnSyncMaxTimes != 0 {
			unSyncMaxTimes = cfg.UnSyncMaxTimes
		}
	}
	log.Info("healthCheck start ", "addr", listenAddr, "inter", checkInterval, "times", unSyncMaxTimes)
	s.wg.Add(1)
	go s.healthCheck()

}

func (s *HealthCheckServer) listen(on bool) error {
	if on {
		listener, err := net.Listen("tcp", listenAddr)
		if err != nil {
			return err
		}
		s.l = listener
		log.Info("healthCheck listen open")
		return nil
	}

	if s.l != nil {
		err := s.l.Close()
		if err != nil {
			return err
		}
		log.Info("healthCheck listen close")
		s.l = nil
	}

	return nil
}

func (s *HealthCheckServer) getHealth(sync bool) (bool, error) {
	reply, err := s.api.IsSync()
	if err != nil {
		return false, err
	}

	peerList, err := s.api.PeerInfo()
	if err != nil {
		return false, err
	}

	log.Debug("healthCheck tick", "peers", len(peerList.Peers), "isCaughtUp", reply.IsOk,
		"health", len(peerList.Peers) > 1 && reply.IsOk, "listen", sync)

	return len(peerList.Peers) > 1 && reply.IsOk, nil
}

func (s *HealthCheckServer) healthCheck() {
	ticker := time.NewTicker(time.Second * time.Duration(checkInterval))
	defer ticker.Stop()
	defer s.wg.Done()

	var sync bool
	var unSyncTimes uint32

	for {
		select {
		case <-s.quit:
			if s.l != nil {
				err := s.l.Close()
				if err != nil {
					log.Error("healthCheck ", "close err ", err)
				}
			}
			if s.api != nil {
				s.api.Close()
			}
			return
		case <-ticker.C:
			health, err := s.getHealth(sync)
			if err != nil {
				continue
			}
			if health {
				if !sync {
					err = s.listen(true)
					if err != nil {
						log.Error("healthCheck ", "listen open err", err.Error())
						continue
					}
					sync = true
				}
				unSyncTimes = 0

			} else {
				if sync {
					if unSyncTimes >= unSyncMaxTimes {
						err = s.listen(false)
						if err != nil {
							log.Error("healthCheck ", "listen close err", err.Error())
							continue
						}
						sync = false
					}
					unSyncTimes++
				}
			}
		}
	}
}