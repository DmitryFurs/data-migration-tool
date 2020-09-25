<?php
/**
 * Copyright © Magento, Inc. All rights reserved.
 * See COPYING.txt for license details.
 */
namespace Migration\ResourceModel;

use Exception;
use Migration\Config;
use Migration\Logger\Logger;
use Migration\Reader\MapInterface;

/**
 * Class DirectCopy
 */
class DirectCopy
{
    /**
     * Direct document copy config key
     */
    private const DIRECT_DOCUMENT_COPY = 'direct_document_copy';

    /**
     * @var Config
     */
    private $config;

    /**
     * @var boolean
     */
    private $directDocumentCopyFlag;

    /**
     * @var Source
     */
    private $source;

    /**
     * @var Destination
     */
    private $destination;

    /**
     * @var Logger
     */
    private $logger;

    /**
     * @var array
     */
    private $columnsCache = [];

    /**
     * @param Config $config
     * @param Source $source
     * @param Destination $destination
     * @param Logger $logger
     */
    public function __construct(
        Config $config,
        Source $source,
        Destination $destination,
        Logger $logger
    ) {
        $this->config = $config;
        $this->directDocumentCopyFlag = (bool)$this->config->getOption(self::DIRECT_DOCUMENT_COPY);
        $this->source = $source;
        $this->destination = $destination;
        $this->logger = $logger;
    }

    /**
     * @param Document $sourceDocument
     * @param Document $destinationDocument
     * @param MapInterface $map
     *
     * @return bool
     */
    public function execute(Document $sourceDocument, Document $destinationDocument, MapInterface $map): bool
    {
        if (!$this->canDirectCopy($sourceDocument, $destinationDocument, $map)) {
            return false;
        }

        $result = true;
        try {
            $this->destination->getAdapter()->insertFromSelect(
                $this->buildDirectCopySelect($sourceDocument, $map),
                $this->destination->addDocumentPrefix($destinationDocument->getName()),
                array_keys($this->getColumnsByMap($sourceDocument, $map))
            );
        } catch (Exception $e) {
            $result = false;
            $this->logger->warning(
                sprintf(
                    'Document %s can not be copied directly because of error: %s',
                    $sourceDocument->getName(),
                    $e->getMessage()
                )
            );
        }

        return $result;
    }

    /**
     * @param Document $sourceDocument
     * @param Document $destinationDocument
     * @param MapInterface $map
     *
     * @return bool
     */
    private function canDirectCopy(Document $sourceDocument, Document $destinationDocument, MapInterface $map)
    {
        return $this->isDirectCopyAllowed()
            && !$this->hasHandlers($sourceDocument, $map, MapInterface::TYPE_SOURCE)
            && !$this->hasHandlers($destinationDocument, $map, MapInterface::TYPE_DEST);
    }

    /**
     * @return bool
     */
    private function isDirectCopyAllowed(): bool
    {
        return $this->directDocumentCopyFlag;
    }

    /**
     * @param Document $document
     * @param MapInterface $map
     * @param string $type
     *
     * @return bool
     */
    private function hasHandlers(Document $document, MapInterface $map, string $type): bool
    {
        $result = false;
        foreach (array_keys($document->getStructure()->getFields()) as $fieldName) {
            $handlerConfig = $map->getHandlerConfigs($document->getName(), $fieldName, $type);
            if (!empty($handlerConfig)) {
                $result = true;
                break;
            }
        }

        return $result;
    }

    /**
     * @param Document $sourceDocument
     * @param MapInterface $map
     *
     * @return \Magento\Framework\DB\Select
     */
    private function buildDirectCopySelect(
        Document $sourceDocument,
        MapInterface $map
    ) {
        $sourceDocumentName = $this->source->addDocumentPrefix($sourceDocument->getName());
        $columns = $this->getColumnsByMap($sourceDocument, $map);
        $schema = $this->config->getSource()['database']['name'];

        return $this->source->getAdapter()->getSelect()->from($sourceDocumentName, $columns, $schema);
    }

    /**
     * @param Document $sourceDocument
     * @param MapInterface $map
     *
     * @return array
     */
    private function getColumnsByMap(Document $sourceDocument, MapInterface $map): array
    {
        $sourceDocumentName = $sourceDocument->getName();
        if (!array_key_exists($sourceDocumentName, $this->columnsCache)) {
            $this->columnsCache[$sourceDocumentName] = [];
            $fieldNameList = array_keys($sourceDocument->getStructure()->getFields());
            foreach ($fieldNameList as $fieldNameSource) {
                $fieldNameDestination = $map->getFieldMap(
                    $sourceDocumentName,
                    $fieldNameSource,
                    MapInterface::TYPE_SOURCE
                );
                if ($fieldNameDestination) {
                    $this->columnsCache[$sourceDocumentName][$fieldNameDestination] = $fieldNameSource;
                }
            }
        }

        return $this->columnsCache[$sourceDocumentName];
    }
}
