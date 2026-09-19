package graphread

// Link is topology only. It carries no amount, effective-payment or identity
// inference. FEC endpoints use bare IDs; an appearance uses its full digest.
type Link struct {
	Family string `json:"family"`
	Key    string `json:"key"`
	From   string `json:"from"`
	To     string `json:"to"`
}

func (l Link) ID() string { return l.Family + ":" + l.Key }
