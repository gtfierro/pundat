package archiver

import (
	"github.com/gtfierro/durandal/common"
)

type MetadataStore interface {
	GetUnitOfTime(VK string, uuid common.UUID) (common.UnitOfTime, error)
	GetMetadata(VK string, tags []string, where common.Dict) (*common.MetadataGroup, error)
	GetDistinct(VK string, tag string, where common.Dict) (*common.MetadataGroup, error)

	SaveMetadata(VK string, records []*common.MetadataRecord) error

	RemoveMetadata(VK string, tags []string, where common.Dict) error
}
