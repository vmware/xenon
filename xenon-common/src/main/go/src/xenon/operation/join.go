// Copyright (c) 2015-2016 VMware, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, without warranties or
// conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
// specific language governing permissions and limitations under the License.

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
