package operation

func JoinN(os []*Operation, n int) ([]*Operation, error) {
	if len(os) == 0 {
		return os, nil
	}

	resc := make(chan *Operation, len(os))
	errc := make(chan error, len(os))

	for _, o := range os {
		go func(o *Operation) {
			<-o.Done()

			err := o.Err()
			if err != nil {
				errc <- err
			} else {
				resc <- o
			}
		}(o)
	}

	reso := make([]*Operation, 0, len(os))
	erro := make([]error, 0, len(os))

	// Wait for n operations to complete
	for {
		select {
		case err := <-errc:
			erro = append(erro, err)
			// Return if we have too many errors
			if len(erro) > (len(os) - n) {
				return nil, erro[0]
			}
		case res := <-resc:
			reso = append(reso, res)
			// Return if we have enough responses
			if len(reso) == n {
				return reso, nil
			}
		}
	}
}

func Join(os []*Operation) ([]*Operation, error) {
	return JoinN(os, len(os))
}
