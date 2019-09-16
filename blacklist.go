package main

var globalAttributeBlacklist = map[string]struct{}{
	"Names": {},
}

var languageAttributeBlacklist = map[string]map[string]struct{}{
	"cpp": {
		"Prop_ReturnValue": struct{}{},
	},
	"go": {
		// "Comments": struct{}{} https://github.com/bblfsh/go-driver/issues/56
		"Imports": struct{}{},
	},
}

func isAttributeBlacklisted(attr string, lang string) bool {
	if _, exists := globalAttributeBlacklist[attr]; exists {
		return true
	} else if attributeBlacklist, exists := languageAttributeBlacklist[lang]; exists {
		if _, exists := attributeBlacklist[attr]; exists {
			return true
		}
	}
	return false
}
