package reportmetadata

import "github.com/vedantadhobley/legal-tender/internal/strictjson"

func strictJSON(raw []byte, target any) error { return strictjson.Decode(raw, target) }
